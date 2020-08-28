"""
Clean and store tabular or GIS files in S3 bucket.
Do parcel-related cleaning and processing here, save to S3, because parcel files are large.

Parcels with different AINs can have the same geometry...is this because of subdivision?
Tag duplicate parcels using centroids, since we don't know which AINs are eventually used for 
entitlements. Store the duplicate counts so that we can get rid of it later, as to not
inflate the number of unique parcel-geometry combinations.

Included: TOC Tiers
        TOC-eligible parcels for 2017 and 2019
        duplicate parcels
        duplicate parcels joined to census tracts and TOC Tiers --> crosswalk_parcels_tracts
"""
import boto3
import intake
import numpy as np
import pandas as pd
import geopandas as gpd
import utils
from datetime import datetime

s3 = boto3.client('s3')
catalog = intake.open_catalog("./catalogs/*.yml")
bucket_name = 'city-planning-entitlements'

time0 = datetime.now()
print(f'Start time: {time0}') 

#------------------------------------------------------------------------#
## TOC Tiers shapefile
#------------------------------------------------------------------------#
gdf = catalog.toc_tiers.read()

gdf = gdf.drop(columns = ['Shape_Leng', 'Shape_Area'])
gdf.rename(columns = {'FINALTIER': 'TOC_Tier'}, inplace = True)

gdf.to_file(driver = 'GeoJSON', filename = './gis/raw/TOC_Tiers.geojson')
s3.upload_file('./gis/raw/TOC_Tiers.geojson', f'{bucket_name}', 'gis/raw/TOC_Tiers.geojson')

time1 = datetime.now()
print(f'Upload TOC Tiers shapefile to S3: {time1 - time0}')


#------------------------------------------------------------------------#
## Upload TOC-eligible parcels
#------------------------------------------------------------------------#
"""
# Make our own TOC-eligible parcels
parcels = gpd.read_file(f'zip+s3://{bucket_name}/gis/raw/la_parcels.zip').to_crs(
                    {'init':'epsg:2229'})
print("Read in file")
parcels['centroid'] = parcels.geometry.centroid
print("Make centroid")
parcels2 = parcels[['AIN', 'centroid']]
parcels2 = gpd.GeoDataFrame(parcels2)
parcels2.crs = {'init':'epsg:2229'}
print("Make into gdf")
parcels2 = parcels2.set_geometry('centroid')

# Doing a spatial join between parcels and TOC tiers is consuming, 
# because TOC tiers are multipolygons.
# Let's use the centroid of the parcel instead and do a spatial join on that.
df1 = gpd.sjoin(parcels2, gdf, how = 'inner', op = 'intersects')

df2 = pd.merge(parcels, df1, on = 'AIN', how = 'left', validate = '1:1')
df2.TOC_Tier = df2.TOC_Tier.fillna('0')
df2['TOC_Tier'] = df2.TOC_Tier.astype(int)

df2 = df2[['AIN', 'geometry', 'TOC_Tier']]
# Zip the shapefile and upload to S3
utils.make_zipped_shapefile(df2, './gis/intermediate/la_parcels_toc')
print("Finish utils")
s3.upload_file('./gis/intermediate/la_parcels_toc.zip', f'{bucket_name}', 
                'gis/intermediate/la_parcels_toc.zip')
"""

# Upload City Planning's version of TOC parcels
parcels = catalog.parcels2014.read().to_crs("EPSG:2229")

tiers = gpd.read_file(f's3://{bucket_name}/gis/raw/TOC_Tiers.geojson')

toc_parcels = catalog.toc_parcels_raw.read()

toc_parcels = (toc_parcels[toc_parcels.BPP != ""][['BPP']]
                .rename(columns = {'BPP':'AIN'})
                .drop_duplicates(subset = 'AIN')
            )

# Attach geometry
toc_parcels = pd.merge(parcels, toc_parcels, how = 'inner', validate = '1:1')

# Doing a spatial join between parcels and TOC tiers is consuming, 
# because TOC tiers are multipolygons.
# Let's use the centroid of the parcel instead and do a spatial join on that.
toc_parcels = (toc_parcels.assign(
        centroid = toc_parcels.geometry.centroid
    ).set_geometry('centroid')
)

# Spatial join with tiers
toc_parcels2 = gpd.sjoin(toc_parcels, tiers, how = 'left', op = 'intersects')

keep = ['AIN', 'TOC_Tier', 'geometry']
toc_parcels2 = (toc_parcels2.set_geometry('geometry')
    .assign(
        TOC_Tier = toc_parcels2.TOC_Tier.fillna(0).astype(int)
    )[keep]
)

# Zip the shapefile and upload to S3
utils.make_zipped_shapefile(toc_parcels2, './gis/intermediate/TOC_Parcels')
s3.upload_file('./gis/intermediate/TOC_Parcels.zip', f'{bucket_name}', 
                'gis/intermediate/TOC_Parcels.zip')

toc_file = "TOC_Parcels"
new = gpd.read_file(f"zip+s3://{bucket_name}/gis/intermediate/{toc_file}.zip")
new.to_parquet(f"./{toc_file}.parquet")
s3.upload_file(f"./{toc_file}.parquet", bucket_name, f"gis/intermediate/{toc_file}.parquet")
os.remove(f"./{toc_file}.parquet")

time2 = datetime.now()
print(f'Upload TOC eligible parcels to S3: {time2 - time1}')


#------------------------------------------------------------------------#
## Tag duplicate parcel geometries
#------------------------------------------------------------------------#
parcels = gpd.read_file(
            f'zip+s3://{bucket_name}/gis/raw/la_parcels.zip').to_crs({'init':'epsg:2229'})

parcels = (parcels[['AIN', 'geometry']]
    .assign(
        parcelsqft = parcels.geometry.area,
        centroid = parcels.geometry.centroid,
        x = parcels.geometry.centroid.x,
        y = parcels.geometry.centroid.y,
    )
)

duplicate_geom = parcels.groupby(['x', 'y']).agg(
                    {'AIN':'count'}).reset_index().rename(columns = {'AIN':'num_AIN'})

parcels2 = pd.merge(parcels, duplicate_geom, 
                on = ['x', 'y'], how = 'left', validate = 'm:1')
parcels2 = parcels2.drop(columns = 'centroid')

utils.make_zipped_shapefile(parcels2, './gis/intermediate/la_parcels_with_dups')
s3.upload_file('./gis/intermediate/la_parcels_with_dups.zip', 
                f'{bucket_name}', 'gis/intermediate/la_parcels_with_dups.zip')

# Upload as geoparquet
parcel_file = "la_parcels_with_dups"
new = gpd.read_file(f"zip+s3://{bucket_name}/gis/intermediate/{parcel_file}.zip")
new.to_parquet(f"./{parcel_file}.parquet")
s3.upload_file(f"./{parcel_file}.parquet", bucket_name, f"gis/intermediate/{parcel_file}.parquet")
os.remove(f"./{parcel_file}.parquet")

time3 = datetime.now()
print(f'Identify duplicate parcel geometries: {time3 - time2}')


#------------------------------------------------------------------------#
## Crosswalk between parcels and census tracts and TOC Tiers
#------------------------------------------------------------------------#
# (1) Import parcels with dups file
parcels = catalog.parcels_with_duplicates.read().to_crs("EPSG:2229")

parcels['centroid'] = parcels.geometry.centroid
parcels2 = parcels.drop(columns = 'geometry').set_geometry('centroid')

# (2) Import TOC Tiers and join parcels to tiers
toc_tiers = gpd.read_file(f's3://{bucket_name}/gis/raw/TOC_Tiers.geojson')

parcels3 = gpd.sjoin(parcels2, toc_tiers, 
            how = 'left', op = 'intersects').drop(columns = 'index_right')

parcels3['TOC_Tier'] = parcels3.TOC_Tier.fillna('0')
parcels3.TOC_Tier = parcels3.TOC_Tier.astype(int)

# (3) Import tracts
tracts = catalog.census_tracts.read()
tracts.rename(columns = {'GEOID10':'GEOID', 'HD01_VD01': 'pop'}, inplace = True)

# (4) Spatial join of parcels to tracts
crosswalk = gpd.sjoin(parcels3, tracts[['GEOID', 'pop', 'geometry']], 
                        how = 'inner', op = 'intersects').drop(columns = 'index_right')

# (5) Aggregate to tract-level; calculate total parcel sqft within tract
"""
Once parcels are joined to tracts, calculate what the total area from the parcels are.
It should be <= 1.
Since tracts include road space, and parcels are just properties,
it's likely that sum(parcelsqft) <= 1 once duplicate parcels are dropped.

Check: Pretty close to sum == 1 (2 tracts have sum < 1; 16 tracts have sum > 1)
"""
parcel_geom = (
        crosswalk
            .drop_duplicates(subset=['x', 'y'], keep='first')
            .groupby('GEOID').agg({'parcelsqft':'sum'}).reset_index()
            .rename(columns = {'parcelsqft':'parcel_tot'})
)

# (6) Merge back in with crosswalk. 
# Now all parcels (incl duplicates) are linked to 1 tract, but sum of parcelsqft is within tract is 1.
parcels4 = pd.merge(crosswalk, parcel_geom, on = 'GEOID', how = 'left', validate = 'm:1')

# (7) Export to S3 as parquet
keep = ['AIN', 'parcelsqft', 'num_AIN', 'parcel_tot', 'TOC_Tier', 'GEOID', 'pop']

parcels4[keep].to_parquet(f's3://{bucket_name}/data/crosswalk_parcels_tracts.parquet')

time4 = datetime.now()
print(f'Crosswalk between parcels and tracts: {time4 - time3}')


#------------------------------------------------------------------------#
## Add TOC-eligibility columns to crosswalk_parcels_tracts
#------------------------------------------------------------------------#
crosswalk_parcels_tracts = pd.read_parquet(
    f's3://{bucket_name}/data/crosswalk_parcels_tracts.parquet')

def toc_tracts_clean_and_aggregate(crosswalk_parcels_tracts):
    # Import data
    toc_parcels = catalog.toc_parcels.read()

    df = pd.merge(crosswalk_parcels_tracts.drop(columns = "pop"), 
         toc_parcels[toc_parcels.TOC_Tier > 0].drop(columns = 'TOC_Tier'), 
         on = 'AIN', how = 'left', validate = '1:1')
    
    # Get rid of duplicate AIN's
    df = df[df.num_AIN == 1]
    
    # Tag if the parcel counts as in TOC tier or not
    def in_tier(row):
        if row.TOC_Tier != 0:
            return 1
        elif row.TOC_Tier == 0:
            return 0

    df = df.assign(
        in_tier = df.apply(in_tier, axis=1)
    )
    
    # Aggregate by in_tier 
    df = (df.groupby(["GEOID", "parcel_tot", "in_tier"])
          .agg({"num_AIN": "sum",
               "parcelsqft":"sum"})
          .reset_index()
         )
    
    # If GEOID has 2 observations, one in_tier==1 and other in_tier==0, let's keep the in_tier==1
    df["obs"] = df.groupby("GEOID").cumcount() + 1
    df["max_obs"] = df.groupby("GEOID")["obs"].transform("max")
    
    df = (df[(df.max_obs == 1) | 
             ((df.in_tier == 1) & (df.max_obs == 2))]
          .drop(columns = ["obs", "max_obs"])
         )
    
    
    # Also, count the total of AIN within each tract
    total_AIN = (crosswalk_parcels_tracts[crosswalk_parcels_tracts.num_AIN == 1]
                    .groupby(['GEOID'])
                    .agg({'num_AIN':'sum'})
                    .rename(columns = {'num_AIN':'total_AIN'})
                    .reset_index()
                   )
    
    # Merge together 
    df2 = pd.merge(df, total_AIN, on = 'GEOID', how = 'left', validate = 'm:1')
    
    # Calculate the % of AIN that falls within TOC tiers and % of area within TOC tiers
    df2 = (df2.assign(
            pct_toc_AIN = df2.num_AIN / df2.total_AIN,
            pct_toc_area = df2.parcelsqft / df2.parcel_tot,
        ).sort_values("GEOID")
           .reset_index(drop=True)
    )
    
    return df2

# We will count tract as being a TOC tract if over 50% of its area or 
# over 50% of its parcels are within a TOC Tier.
def set_groups(df):
    cutoff_AIN = 0.5
    cutoff_area = 0.5
    
    def set_cutoffs(row):
        toc_AIN = 0
        toc_area = 0
        
        if (row.in_tier == 1) & (row.pct_toc_AIN >= cutoff_AIN):
            toc_AIN = 1
        if (row.in_tier == 1) & (row.pct_toc_area >= cutoff_area):
            toc_area = 1
        
        return pd.Series([toc_AIN, toc_area], 
                         index=['toc_AIN', 'toc_area'])
    
    with_cutoffs = df.apply(set_cutoffs, axis=1)
    
    df = pd.concat([df, with_cutoffs], axis=1)
    
    return df


df1 = toc_tracts_clean_and_aggregate(crosswalk_parcels_tracts)
df2 = set_groups(df1)

# Clean up columns and merge with crosswalk
keep = ["GEOID", "total_AIN", 
        "pct_toc_AIN", "pct_toc_area", 
        "toc_AIN", "toc_area"]

df = (pd.merge(crosswalk_parcels_tracts, df2[keep], 
              on = "GEOID", how = "left", validate = "m:1")
      .sort_values(["GEOID", "AIN"])
      .reset_index(drop=True)
     )

df.to_parquet(f's3://{bucket_name}/data/crosswalk_parcels_tracts.parquet')

#time5 = datetime.now()
#print(f'Add TOC eligibility to crosswalk: {time5 - time4}')   
#print(f'Total execution time: {time5 - time0}')
# Clean and store tabular or GIS files in S3 bucket
# Used for files that were sent over email and need very little processing
# Included: TOC Tiers, parcels joined to TOC Tiers, crosswalks for zoning and PCTS parsers
import intake
import numpy as np
import pandas as pd
import geopandas as gpd
import boto3
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
gdf = gpd.read_file(f'zip+s3://{bucket_name}/gis/source/TOC_Tiers_Oct2017.zip')

gdf = gdf.drop(columns = ['Shape_Leng', 'Shape_Area'])
gdf.rename(columns = {'FINALTIER': 'TOC_Tier'}, inplace = True)

gdf.to_file(driver = 'GeoJSON', filename = './gis/raw/TOC_Tiers.geojson')
s3.upload_file('./gis/raw/TOC_Tiers.geojson', f'{bucket_name}', 'gis/raw/TOC_Tiers.geojson')

time1 = datetime.now()
print(f'Upload TOC Tiers shapefile to S3: {time1 - time0}')


#------------------------------------------------------------------------#
## TOC-eligible parcels
#------------------------------------------------------------------------#
parcels = gpd.read_file(f'zip+s3://{bucket_name}/gis/raw/la_parcels.zip').to_crs({'init':'epsg:2229'})

parcels['centroid'] = parcels.geometry.centroid

parcels2 = parcels[['AIN', 'centroid']]
parcels2 = gpd.GeoDataFrame(parcels2)
parcels2.crs = {'init':'epsg:2229'}

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
s3.upload_file('./gis/intermediate/la_parcels_toc.zip', f'{bucket_name}', 'gis/intermediate/la_parcels_toc.zip')

time2 = datetime.now()
print(f'Identify TOC eligible parcels: {time2 - time1}')


#------------------------------------------------------------------------#
## Zoning Parser
#------------------------------------------------------------------------#
"""
Use with notebooks/utils.py ZoningInfo data class
After the zoning string is parsed, there were still observations that failed to be parsed.
Those were manually coded. 
Save the failed to be parsed codebook and general codebook for zoning string into S3.
"""
# Import the codebook we want to use for parse fails
df = pd.read_excel(f's3://{bucket_name}/references/Zoning_Parser_Codebook.xlsx', sheet_name = 'use_for_parse_fails')

# Make sure column types are the same as the other observations
col_names = ['Q', 'T', 'D']
df[col_names] = df[col_names].astype(bool)


for col in ['zone_class', 'specific_plan', 'height_district']:
    df[col] = df[col].fillna('')
    df[col] = df[col].astype(str)


df.to_parquet(f's3://{bucket_name}/data/crosswalk_zone_parse_fails.parquet')


# Import other sheets in the codebook and upload to S3
for name in ['zone_class', 'supplemental_use_overlay', 'specific_plan']:
    df = pd.read_excel(f's3://{bucket_name}/references/Zoning_Parser_Codebook.xlsx', sheet_name = f'{name}')
    df.to_parquet(f's3://{bucket_name}/data/crosswalk_{name}.parquet')

time3 = datetime.now()
print(f'Zone parser crosswalk: {time3 - time2}')


#------------------------------------------------------------------------#
## PCTS Parser
#------------------------------------------------------------------------#
# Import the sheets in the codebook and upload to S3
for name in ['Prefix', 'Suffix']:
    new_name = f'{name.lower()}'
    df = pd.read_excel(f's3://{bucket_name}/references/PCTS_Parser_Codebook.xlsx', sheet_name = f'{name}')
    df.drop(columns = 'notes').to_parquet(f's3://{bucket_name}/data/crosswalk_{new_name}.parquet')

time4 = datetime.now()
print(f'PCTS parser crosswalk: {time4 - time3}')


#------------------------------------------------------------------------#
## Crosswalk between parcels and census tracts
#------------------------------------------------------------------------#
tracts = catalog.census_tracts.read()
tracts.rename(columns = {'GEOID10':'GEOID', 'HD01_VD01': 'pop'}, inplace = True)


crosswalk = gpd.sjoin(parcels2[['AIN', 'centroid']], 
                        tracts[['GEOID', 'pop', 'geometry']], 
                        how = 'inner', op = 'intersects').drop(columns = 'index_right')
# Export to S3
crosswalk[['AIN', 'GEOID', 'pop']].to_parquet(f's3://{bucket_name}/data/crosswalk_parcels_tracts.parquet')

#time5 = datetime.now()
#print(f'Make parcels to tracts crosswalk: {time5 - time4}')


#------------------------------------------------------------------------#
## Crosswalk between census tracts and TOC tiers
#------------------------------------------------------------------------#
tracts = catalog.census_tracts.read().to_crs({'init':'epsg:4326'})
tracts = tracts[['GEOID10', 'geometry']]

toc_tiers = utils.reconstruct_toc_tiers_file()

def tracts_tiers_intersection(
    gdf: gpd.GeoDataFrame,
    toc_tiers: gpd.GeoDataFrame,
    tier: int,
) -> gpd.GeoDataFrame:

    assert tier >= 1 and tier <= 4
    current = gdf
    # trigger a spatial index build on the current df
    current.sindex
    colname = f"tier_{tier}"
    toc_tiers = toc_tiers.set_geometry(colname).drop(columns=["geometry"])
    toc_tiers = toc_tiers[~toc_tiers.is_empty]
    
    current = gpd.overlay(current, toc_tiers, how="intersection")
    current['intersect_tier'] = tier
    
    keep_col = ['GEOID10', 'tiers_id', 'intersect_tier', 'geometry']
    current = current[keep_col].rename(columns = {'GEOID10':'GEOID'})
    
    return current

# Get the intersections for each tier
t1 = tracts_tiers_intersection(tracts, toc_tiers, 1)
t2 = tracts_tiers_intersection(tracts, toc_tiers, 2)
t3 = tracts_tiers_intersection(tracts, toc_tiers, 3)
t4 = tracts_tiers_intersection(tracts, toc_tiers, 4)

df = pd.concat([
    t1,
    t2,
    t3,
    t4,
], sort = False)

"""
Keep max tier only
But, allow for the possibility that tract falls into 2 diff tiers, just one time each,
and are distinct areas (not overlapping).
We want to keep both observations, while getting rid of the rest.
"""
df = (
    df.to_crs({'init':'epsg:2229'})
    .assign(intersect_sqft = df.geometry.area)
)


df['max_tier'] = df.groupby(['GEOID', 'intersect_sqft'])['intersect_tier'].transform('max')
df = df[df.max_tier == df.intersect_tier]

keep = ['GEOID', 'tiers_id', 'intersect_tier', 'intersect_sqft']
df = df[keep].reset_index(drop = True).to_parquet(
            f's3://{bucket_name}/data/crosswalk_tracts_toc_tiers.parquet')


time6 = datetime.now()
print(f'Make tracts to toc tiers crosswalk: {time6 - time5}')
print(f'Total execution time: {time6 - time0}')

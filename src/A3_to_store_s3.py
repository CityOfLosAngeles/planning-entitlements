"""
Clean and store tabular or GIS files in S3 bucket.
Used for files that were sent over email and need very little processing.
Do parcel-related cleaning and processing here, save to S3, because parcel files are large.
Included: TOC Tiers
        TOC-eligible parcels for 2017 and 2019
        duplicate parcels
        2017 TOC-eligible parcels joined to census tracts
"""
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
## Upload TOC-eligible parcels
#------------------------------------------------------------------------#
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
print("FInish utils")
s3.upload_file('./gis/intermediate/la_parcels_toc.zip', f'{bucket_name}', 
                'gis/intermediate/la_parcels_toc.zip')


# Upload crosswalk given by City Planning
for y in ['2017', '2019']:
    df = (gpd.read_file(
            f's3://{bucket_name}/gis/source/city_{y}TOC_parcels/')
            .to_crs({'init':'epsg:2229'})
    )
    keep_cols = ['AIN', 'TOC_Tier']
    if y=='2017':
        df.rename(columns = {"Tier_Int":"TOC_Tier"}, inplace=True)
    if y=='2019':
        df.rename(columns = {"Tier":"TOC_Tier"}, inplace=True)
    df = df[keep_cols].drop_duplicates()
    df.to_parquet(f's3://{bucket_name}/data/crosswalk_toc{y}_parcels.parquet')

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

time3 = datetime.now()
print(f'Identify duplicate parcel geometries: {time3 - time2}')


#------------------------------------------------------------------------#
## Create TOC-eligible parcels joined to tracts
#------------------------------------------------------------------------#
parcels_with_dups = gpd.read_file(
            f'zip+s3://{bucket_name}/gis/intermediate/la_parcels_with_dups.zip')

crosswalk_parcels_tracts = pd.read_parquet(
                f's3://{bucket_name}/data/crosswalk_parcels_tracts.parquet')

toc_parcels = pd.read_parquet(
            f's3://{bucket_name}/data/crosswalk_toc2017_parcels.parquet')

parcels2 = pd.merge(parcels_with_dups, crosswalk_parcels_tracts, on = 'AIN', validate = '1:1')

"""
Once parcels are joined to tracts, calculate what the total area from the parcels are.
It should be <= 1.
Since tracts include road space, and parcels are just properties,
it's likely that sum(parcelsqft) < 1.
"""
parcel_geom = (
        parcels2
            .drop_duplicates(subset=['x', 'y'], keep='first')
            .groupby('GEOID').agg({'parcelsqft':'sum'}).reset_index()
            .rename(columns = {'parcelsqft':'parcel_tot'})
)

parcels3 = pd.merge(parcels2, parcel_geom, on = 'GEOID', how = 'left', validate = 'm:1')
parcels4 = parcels3[parcels3.AIN.isin(toc_parcels.AIN)]


utils.make_zipped_shapefile(parcels4, './gis/intermediate/toc_parcels_tracts')
s3.upload_file('./gis/intermediate/toc_parcels_tracts.zip', 
                f'{bucket_name}', 'gis/intermediate/toc_parcels_tracts.zip')


time4 = datetime.now()
print(f'Identify TOC-eligible parcels joined to tracts: {time4 - time3}')
print(f'Total execution time: {time4 - time0}')
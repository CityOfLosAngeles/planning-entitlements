# Combine Assessor Parcels 2014 shapefile with the updated 2019 Assessor Parcel information
"""
Assessor parcel information might change year to year
Shapefile only provided with 2014 data
2019 data is available as csv
Use the shapefile from 2014 and attach 2019 information
Save this file to catalog to use for analysis
"""

import numpy as numpy
import pandas as pd
import geopandas as gpd
import intake
import utils
import boto3
from datetime import datetime

catalog = intake.open_catalog("./catalogs/*.yml")
bucket_name = 'city-planning-entitlements'
s3 = boto3.client('s3')

time0 = datetime.now()
print(f'Start time: {time0}')


# Import parcels shapefile
gdf = gpd.read_file(f'zip+s3://{bucket_name}/gis/source/Parcels_2014.zip')

gdf = gdf[gdf.geometry.notna()]

# Subset and keep only obs in City of LA
gdf = gdf[gdf.SITUSCITY == 'LOS ANGELES CA']
gdf = gdf[['AIN', 'geometry']]


# Zip and upload just the City of LA parcels to S3
utils.make_zipped_shapefile(gdf, './gis/raw/la_parcels')
s3.upload_file('./gis/raw/la_parcels.zip', bucket_name, 'gis/raw/la_parcels.zip')


# Import 2019 Tax Assessor parcel data
df = pd.read_csv(f's3://{bucket_name}/gis/source/Assessor_Parcels_Data_2019.csv')
df.to_parquet(f's3://{bucket_name}/data/source/Assessor_Parcels_Data.parquet')


# Only keep certain columns 
keep = ['AIN', 'PropertyLocation', 'GeneralUseType', 'SpecificUseType']
df = df[keep]


# Merge using APN
merged_df = pd.merge(gdf, df, on = 'AIN', how = 'inner', validate = '1:1')


# Export to S3 and add to catalog



time1 = datetime.now()
print(f'Total execution time: {time1 - time0}')
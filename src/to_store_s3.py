# Clean and store tabular or GIS files in S3 bucket
# Used for files that were sent over email and need very little processing
# Included; TOC Tiers, parcels joined to TOC Tiers
import numpy as np
import pandas as pd
import geopandas as gpd
import boto3
import utils
from datetime import datetime

s3 = boto3.client('s3')
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
print(f'Total execution time: {time2 - time0}')
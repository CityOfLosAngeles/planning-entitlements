import boto3
import geopandas as gpd
import pandas as pd
import utils

s3 = boto3.client('s3')
bucket_name = 'city-planning-entitlements'
parcels = gpd.read_file(f'zip+s3://{bucket_name}/gis/raw/la_parcels.zip').to_crs(
                    {'init':'epsg:2229'})

tiers = gpd.read_file(f's3://{bucket_name}/gis/raw/TOC_Tiers.geojson')

toc_parcels = gpd.read_file(f'zip+s3://{bucket_name}/gis/source/TOC_Parcels.zip')

toc_parcels = (toc_parcels[toc_parcels.BPP != ""][['BPP']]
                .rename(columns = {'BPP':'AIN'})
                .drop_duplicates(subset = 'AIN')
            )

# Attach geometry
toc_parcels = pd.merge(parcels, toc_parcels, how = 'inner', validate = '1:1')

# Spatial join with tiers
toc_parcels2 = gpd.sjoin(toc_parcels, tiers, how = 'left', op = 'intersects')

keep = ['AIN', 'TOC_Tier', 'geometry']
toc_parcels2 = toc_parcels2[keep]

print(toc_parcels2.columns)

# Zip the shapefile and upload to S3
utils.make_zipped_shapefile(toc_parcels2, './gis/intermediate/TOC_Parcels')
s3.upload_file('./gis/intermediate/TOC_Parcels.zip', f'{bucket_name}', 
                'gis/intermediate/TOC_Parcels.zip')

"""
Upload TOC Tiers and TOC-eligible parcels
Included: TOC Tiers
        TOC-eligible parcels for 2017 and 2019
"""
import boto3
import geopandas as gpd
import intake
import numpy as np
import pandas as pd
import utils

from datetime import datetime

s3 = boto3.client('s3')
catalog = intake.open_catalog("./catalogs/*.yml")
bucket_name = 'city-planning-entitlements'

#------------------------------------------------------------------------#
## TOC Tiers shapefile
#------------------------------------------------------------------------#
gdf = catalog.toc_tiers.read()

gdf = gdf.drop(columns = ['Shape_Leng', 'Shape_Area'])
gdf.rename(columns = {'FINALTIER': 'TOC_Tier'}, inplace = True)

gdf.to_file(driver = 'GeoJSON', filename = './gis/raw/TOC_Tiers.geojson')
s3.upload_file('./gis/raw/TOC_Tiers.geojson', bucket_name, 'gis/raw/TOC_Tiers.geojson')

time1 = datetime.now()
print(f'Upload TOC Tiers shapefile to S3: {time1 - time0}')


#------------------------------------------------------------------------#
## Upload TOC-eligible parcels
#------------------------------------------------------------------------#
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
file_name = "TOC_Parcels"

utils.make_zipped_shapefile(toc_parcels2, f'./gis/intermediate/{file_name}')
s3.upload_file(f'./gis/intermediate/{file_name}.zip', 
                bucket_name, f'gis/intermediate/{file_name}.zip')

time2 = datetime.now()
print(f'Upload TOC eligible parcels to S3: {time2 - time1}')

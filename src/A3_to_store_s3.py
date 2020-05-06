"""
Clean and store tabular or GIS files in S3 bucket.
Used for files that were sent over email and need very little processing.
Included: TOC Tiers, parcels joined to TOC Tiers, 
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
## TOC-eligible parcels
#------------------------------------------------------------------------#
parcels = gpd.read_file(
            f'zip+s3://{bucket_name}/gis/raw/la_parcels.zip').to_crs({'init':'epsg:2229'})

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
s3.upload_file('./gis/intermediate/la_parcels_toc.zip', 
                f'{bucket_name}', 'gis/intermediate/la_parcels_toc.zip')

time2 = datetime.now()
print(f'Identify TOC eligible parcels: {time2 - time1}')


#------------------------------------------------------------------------#
## Tag duplicate parcel geometries
#------------------------------------------------------------------------#
la_parcels = gpd.read_file(
            f'zip+s3://{bucket_name}/gis/raw/la_parcels.zip').to_crs({'init':'epsg:2229'})

la_parcels = (la_parcels[['AIN', 'geometry']]
    .assign(
        parcel_sqft = la_parcels.geometry.area,
        centroid = la_parcels.geometry.centroid,
        x = la_parcels.geometry.centroid.x,
        y = la_parcels.geometry.centroid.y,
    )
    .rename(columns = {'geometry':'parcel_geom'})
)

duplicate_geom = la_parcels.groupby(['x', 'y']).agg({'AIN':'count'}).reset_index()
duplicate_geom.rename(columns = {'AIN':'num_AIN'}, inplace = True)

la_parcels2 = pd.merge(la_parcels, duplicate_geom, 
                on = ['x', 'y'], how = 'left', validate = 'm:1')
la_parcels2 = (la_parcels2
    .drop(columns = 'centroid')
    .rename(columns = {'parcel_geom':'geometry'
})

utils.make_zipped_shapefile(la_parcels2, './gis/intermediate/la_parcels_with_dups')

s3.upload_file('./gis/intermediate/la_parcels_with_dups.zip', 
               f'{bucket_name}', 'gis/intermediate/la_parcels_with_dups.zip')

time3 = datetime.now()
print(f'Identify duplicate parcel geometries: {time3 - time2}')



#------------------------------------------------------------------------#
## Test -- get rid of later
#------------------------------------------------------------------------#
parcels = gpd.read_file(
            f'zip+s3://{bucket_name}/gis/intermediate/la_parcels_with_dups.zip')

parcels = (parcels
    .to_crs({'init':'epsg:2229'})
    .assign(centroid = parcels.geometry.centroid)
    .set_geometry('centroid')
)

crosswalk_parcels_tracts = pd.read_parquet(
                f's3://{bucket_name}/data/crosswalk_parcels_tracts.parquet')

toc_tiers = utils.reconstruct_toc_tiers_file()


# Find which parcels fall within TOC Tiers
def 
for col in ["tier_1", "tier_2", "tier_3", "tier_4"]:
        df = df.set_geometry(col)
        df[col] = df.apply(lambda row: 
                           shapely.geometry.GeometryCollection() if row[col] is None 
                           else row[col], axis = 1)
        df = df.set_geometry(col)
gpd.sjoin(parcels,  



parcels2 = pd.merge(la_parcels_with_dups, crosswalk_parcels_tracts, on = 'AIN', validate = '1:1')



utils.make_zipped_shapefile(parcels2, './gis/intermediate/test_parcels_to_tracts')
s3.upload_file('./gis/intermediate/test_parcels_to_tracts.zip', 
                f'{bucket_name}', 'gis/intermediate/test_parcels_to_tracts.zip')

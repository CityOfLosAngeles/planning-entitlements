# Clean up Assessor Parcels 2014 shapefile and 2019 Assessor Parcel information
"""
Assessor parcel information might change year to year

This is LA County's 2006-2019 parcels
https://data.lacounty.gov/Parcel-/Assessor-Parcels-Data-2006-thru-2019/9trm-uz8i

Think about how to match a bunch of centroids (over time) against a polygon (2014 or 2017)

Save the City of LA parcels as zipped shapefile
Save the 2019 Assessor data separately as parquet 
"""
import boto3
import geopandas as gpd
import intake
import numpy as numpy
import pandas as pd
import utils
from datetime import datetime

catalog = intake.open_catalog("./catalogs/*.yml")
bucket_name = 'city-planning-entitlements'
s3 = boto3.client('s3')

time0 = datetime.now()
print(f'Start time: {time0}')


#----------------------------------------------------------------------#
# Historical parcels
#----------------------------------------------------------------------#
"""
Get historical parcels, so we can track AINs for a given
parcel over time.
"""
def store_as_parquet():
    
    df = (pd.read_csv(
        f's3://{bucket_name}/data/source/Assessor_Parcels_Data_2006_2019.csv')
        .drop(columns = ["Location_1"])
    )
    
    print(list(df.columns))
    
    # Write full version as 3 parquets
    for i in [2005, 2010, 2015]:
        time0 = datetime.now()
        print(i)
        start_year = i + 1
        end_year = i + 5
        df[(df.RollYear >= start_year) & (df.RollYear <= end_year)].to_parquet(
            f"s3://{bucket_name}/data/source/Assessor_Parcels_Data_{start_year}_{end_year}.parquet"
        )
        
        time1 = datetime.now()
        print(f'Write {start_year}-{end_year} parquet: {time1 - time0}')


def clean_parcels(file_year_name):
    print(file_year_name)
    time0 = datetime.now()
    df = pd.read_parquet(
        f"s3://{bucket_name}/data/source/Assessor_Parcels_Data_{file_year_name}.parquet")

    # Subset to City of LA
    df = df[df.City=="LOS ANGELES CA"]
    
    time1 = datetime.now()
    print(f'Subset to City: {time1 - time0}')


    # Keep only a couple columns, drop duplicates based on centroid
    keep = ["AIN", "AssessorID", "CENTER_LAT", "CENTER_LON"]
    df = df[keep].drop_duplicates()
    
    # See how many AssessorID (string) contains letters that make AIN (numeric) null
    missing_ain = df[(df.AIN.isna()) and (df.AssessorID.notna())]
    print(f'# rows where AIN is missing: {len(missing_ain)}')
    print(f'# unique parcels where AIN is missing: {missing_ain.AssessorID.nunique()}')

    # Export to S3
    df.to_parquet(f's3://{bucket_name}/raw/Assessor_Parcels_{file_year_name}.parquet')

    time2 = datetime.now()
    print(f'Write cleaned up df as parquet: {time2 - time1}')


store_as_parquet()
#clean_parcels("2006-2010")
#clean_parcels("2011-2015")
#clean_parcels("2016-2019")

'''
#----------------------------------------------------------------------#
# Parcels shapefile
#----------------------------------------------------------------------#
gdf = gpd.read_file(f'zip+s3://{bucket_name}/gis/source/Parcels_2014.zip')

gdf = gdf[gdf.geometry.notna()]

# Subset and keep only obs in City of LA
gdf = gdf[gdf.SITUSCITY == 'LOS ANGELES CA']
gdf = gdf[['AIN', 'geometry']]
gdf = gdf.drop_duplicates()


# There are still duplicates - dissolve and create multipolygons for those obs. 
gdf2 = gdf.dissolve(by = 'AIN').reset_index()


# Export to S3 and add to catalog
parcel_file = 'gis/raw/la_parcels'
utils.make_zipped_shapefile(gdf2, f'./{parcel_file}')
s3.upload_file(f'./{parcel_file}.zip', bucket_name, f'{parcel_file}.zip')

# Write as geoparquet
new = gpd.read_file(f"zip+s3://{bucket_name}/gis/raw/la_parcels.zip")
new.to_parquet(f'./{parcel_file}.parquet')
s3.upload_file(f'./{parcel_file}.parquet', bucket_name, f'{parcel_file}.parquet')


#----------------------------------------------------------------------#
# 2019 Assessor Parcels Data
#----------------------------------------------------------------------#
df = pd.read_csv(f's3://{bucket_name}/data/source/Assessor_Parcels_Data_2019.csv')
gdf_ain = gdf2[['AIN']]


# Merge with parcels that are in City of LA to pare down our obs
df['AIN'] = df.AIN.astype('str')
df = pd.merge(df, gdf_ain, on = 'AIN', how = 'inner', validate = '1:1')


# Remove characters that prevent it from being converted to numeric
for col in ['Units', 'FixtureValue', 'FixtureExemption', 'PersonalPropertyValue', 'PersonalPropertyExemption', 
            'AdministrativeRegion', 'HouseFraction', 'StreetDirection', 'UnitNo']:
    df[col] = df[col].str.replace(',', '')
    df[col] = df[col].str.replace('$', '')
    df[col] = df[col].str.replace('  ', '')

# Fix data types
for col in ['Units', 'FixtureValue', 'FixtureExemption', 'PersonalPropertyValue', 'PersonalPropertyExemption' ]:
    df[col] = df[col].astype(float)

for col in ['AdministrativeRegion' ,'HouseFraction', 'StreetDirection', 'UnitNo']:
    df[col] = df[col].astype('str')


df.to_parquet(f's3://{bucket_name}/data/raw/Assessor_Parcels_2019_full.parquet')


# Only keep certain columns 
keep = ['AIN', 'PropertyLocation', 'GeneralUseType', 'SpecificUseType']
df1 = df[keep]

df1.to_parquet(f's3://{bucket_name}/data/raw/Assessor_Parcels_2019_abbrev.parquet')


time1 = datetime.now()
print(f'Total execution time: {time1 - time0}')
'''
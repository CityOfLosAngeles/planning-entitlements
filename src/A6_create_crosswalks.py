"""
Create and store crosswalk files in S3 bucket.
Included: 
        crosswalks for zoning and PCTS parsers,
        crosswalk for parcels to tracts,
        crosswalk for parcels that are RSO units,
        crosswalk for tracts and % of AIN that belong to each zone_class
"""
import boto3
import geopandas as gpd
import os
import pandas as pd
import utils

from datetime import datetime

s3 = boto3.client('s3')
bucket_name = 'city-planning-entitlements'

#------------------------------------------------------------------------#
## Zoning Parser
#------------------------------------------------------------------------#
"""
Use with laplan.zoning ZoningInfo data class
After the zoning string is parsed, there were still observations that failed to be parsed.
Those were manually coded. 
Save the failed to be parsed codebook and general codebook for zoning string into S3.
"""
# Import the codebook we want to use for parse fails
df = pd.read_excel(f's3://{bucket_name}/references/Zoning_Parser_Codebook.xlsx', 
                    sheet_name = 'use_for_parse_fails')

# Make sure column types are the same as the other observations
col_names = ['Q', 'T', 'D']
df[col_names] = df[col_names].astype(bool)

for col in ['zone_class', 'specific_plan', 'height_district']:
    df[col] = df[col].fillna('')
    df[col] = df[col].astype(str)

df.to_parquet(f's3://{bucket_name}/data/crosswalk_zone_parse_fails.parquet')


# Import other sheets in the codebook and upload to S3
for name in ['zone_class', 'supplemental_use_overlay', 'specific_plan']:
    df = pd.read_excel(f's3://{bucket_name}/references/Zoning_Parser_Codebook.xlsx', 
                        sheet_name = f'{name}')
    df.to_parquet(f's3://{bucket_name}/data/crosswalk_{name}.parquet')


#------------------------------------------------------------------------#
## PCTS Parser
#------------------------------------------------------------------------#
# Import the sheets in the codebook and upload to S3
for name in ['Prefix', 'Suffix']:
    new_name = f'{name.lower()}'
    df = pd.read_excel(f's3://{bucket_name}/references/PCTS_Parser_Codebook.xlsx', 
                        sheet_name = f'{name}')
    df.drop(columns = 'notes').to_parquet(
            f's3://{bucket_name}/data/crosswalk_{new_name}.parquet')


#------------------------------------------------------------------------#
## APNs with RSO units
#------------------------------------------------------------------------#
df = pd.read_excel(f's3://{bucket_name}/references/RSO_Properties_6_20_19.xlsx', 
                sheet_name = 'ZIMAS_APN_Parcel_List')

df = df.assign(AIN = df.APN.astype(str))

df = df[df.RSO_Inventory == "Yes"]

keep_cols = ['AIN', 'Parcel_PIN', 'RSO_Units', 'Category']

df[keep_cols].to_parquet(f's3://{bucket_name}/data/crosswalk_parcels_rso.parquet')


#------------------------------------------------------------------------#
## APNs with zone class
#------------------------------------------------------------------------#
def join_parcels_to_zones():
    # (1) Import zoning, dissolve by zone_class (which is more specific than zone_summary)
    time0 = datetime.now()
    print("Start join_parcels_to_zones")

    zoning = gpd.read_file(f"zip+s3://{bucket_name}/gis/raw/parsed_zoning.zip")
    zone_class = zoning[["zone_class", "geometry"]]

    time1 = datetime.now()
    print(f'Read in zoning and dissolve: {time1 - time0}')

    # (2) Get parcel centroids
    parcel_geom = pd.read_parquet(
        f"s3://{bucket_name}/data/crosswalk_parcels_tracts_lacity.parquet")

    parcel_geom = gpd.GeoDataFrame(
            parcel_geom,
            geometry=gpd.points_from_xy(parcel_geom.x, parcel_geom.y),
            crs="EPSG:4326",
        )

    time2 = datetime.now()
    print(f'Read in parcels and grab centroids: {time2 - time1}')

    # (3) Spatial join between parcel centroids and zone_class 
    gdf = gpd.sjoin(parcel_geom.to_crs("EPSG:4326"), 
                    zone_class.to_crs("EPSG:4326"), 
                    how = "inner", op = "intersects").drop(columns = "index_right")

    time3 = datetime.now()
    print(f'Spatial join: {time3 - time2}')

    # (4) Drop duplicate parcels
    gdf = (gdf.drop_duplicates(subset = ["uuid", "zone_class"])
        .reset_index(drop=True)
        .drop(columns = ['x', 'y', 'AIN', 'num_AIN'])
    )

    time4 = datetime.now()
    print(f'Drop dups: {time4 - time3}')

    utils.upload_geoparquet(gdf, file_name="parcels_joined_zones.parquet", 
                        S3_path="gis/intermediate/")
    
    time5 = datetime.now()
    print(f'Upload geoparquet to S3: {time5 - time4}')
    print(f'Total time for join_parcels_to_zones: {time5 - time0}')

    return gdf


# (5) Aggregate to tract
def make_tract_level(gdf):
    time0 = datetime.now()
    print(f'Start make_tract_level: {time0}')

    # Aggregate to tract-tier-zone_class
    tract_zone_cols = ["GEOID", "TOC_Tier", "total_AIN", "zone_class"]
    by_zone_tract = (gdf.groupby(tract_zone_cols)
                    .agg({"uuid": "count"})
                    .reset_index()
                    )

    time1 = datetime.now()
    print(f'1st aggregation: {time1 - time0}')

    # Make wide
    tract_tier_cols = ["GEOID", "TOC_Tier", "total_AIN"]
    by_tract_tier = (by_zone_tract.pivot(
                        index = tract_tier_cols, 
                        columns = "zone_class", values = "uuid"
                        )
                .reset_index()
                )

    by_tract_tier.columns.name = None

    time2 = datetime.now()
    print(f'2nd aggregation: {time2 - time1}')

    # Aggregate to tract
    # Ignore the fact that parcels can fall into different tiers within same tract
    tract_cols = ["GEOID", "total_AIN"]
    by_tract = (by_tract_tier.drop(columns = "TOC_Tier")
                .pivot_table(index = tract_cols, aggfunc = "sum")
                .sort_values("GEOID")
                .reset_index()
            )
    
    time3 = datetime.now()
    print(f'3rd aggregation: {time3 - time2}')

    # Create columns that tell us what % AIN belong to each zone_class
    remove_me = ["GEOID", "total_AIN"]
    zone_cols = [x for x in list(by_tract.columns) if x not in remove_me]

    for c in zone_cols:
        by_tract[c] = by_tract[c] / by_tract["total_AIN"]
    
    by_tract = (by_tract.sort_values("GEOID")
                .reset_index(drop=True)
    )
    
    # Export to S3
    by_tract.to_parquet(f"s3://{bucket_name}/data/crosswalk_tracts_zone_class.parquet")
    
    time4 = datetime.now()
    print(f'Finish and export: {time4 - time3}')
    print(f'Total time for make_tract_level: {time4 - time0}')
   
    return by_tract

gdf = join_parcels_to_zones()
final = make_tract_level(gdf)
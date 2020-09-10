# Import and do light cleaning on spatial datasets

import boto3
import geopandas as gpd 
import os
import pandas as pd 

s3 = boto3.client("s3")
bucket_name = "city-planning-entitlements"

# This census tracts file came from our risk management S3
census_tracts = gpd.read_file(f"s3://{bucket_name}/gis/raw/census_tracts_withpop.geojson")

# Drop excess columns, rename to match our other datasets
census_tracts = (census_tracts[["GEOID10", "geometry"]]
                    .rename(columns = {"GEOID10": "GEOID"})
                    .to_crs("EPSG:2229")
                    .sort_values("GEOID")
                    .reset_index(drop=True)
                )

census_tracts.to_file(driver = "GeoJSON", filename = "census_tracts.geojson")
s3.upload_file("census_tracts.geojson", bucket_name, "gis/raw/census_tracts.geojson")
os.remove("census_tracts.geojson")
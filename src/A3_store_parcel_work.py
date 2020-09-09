"""
Continue parcel-related cleaning and processing here, 
save to S3, because parcel files are large.
Moving to historical 2006-2019 parcels means we only have parcel centroids.
Will NOT be using parcel polygons, which we have for 2014.
Parcels with different AINs can have the same geometry...is this because of subdivision?
Tag duplicate parcels using centroids, since we don't know which AINs are eventually used for 
entitlements. Store the duplicate counts so that we can get rid of it later, as to not
inflate the number of unique parcel-geometry combinations.
Included: 
        duplicate parcels
        duplicate parcels joined to census tracts and TOC Tiers --> crosswalk_parcels_tracts
"""
import boto3
import geopandas as gpd
import intake
import numpy as np
import os
import pandas as pd
import uuid

s3 = boto3.client('s3')
catalog = intake.open_catalog("./catalogs/*.yml")
bucket_name = 'city-planning-entitlements'

'''
#------------------------------------------------------------------------#
## Tag duplicate parcel geometries
#------------------------------------------------------------------------#
def tag_duplicate_parcels():
    # Download geoparquet
    file_name = "lacounty_parcels"
    s3.download_file(bucket_name, 
                        f"gis/intermediate/{file_name}.parquet", 
                        f'{file_name}.parquet')

    parcels = gpd.read_parquet(f'{file_name}.parquet')
    os.remove(f'{file_name}.parquet')

    # Keep columns needed for crosswalk
    keep = ["AIN", "GEOID", "CENTER_LON", "CENTER_LAT"]

    parcels = (parcels.reset_index()
            [keep]
            .sort_values(["GEOID", "AIN"])
            .reset_index(drop=True)
            # Assign the X, Y points as strings, so we correctly identify duplicates
            .assign(
                x = parcels.CENTER_LON.round(decimals=7).astype(str), 
                y = parcels.CENTER_LAT.round(decimals=7).astype(str),
            ).dropna(subset = ["CENTER_LON", "CENTER_LAT"])
            .drop(columns = ["CENTER_LON", "CENTER_LAT"])
    )

    duplicate_geom = (parcels.groupby(['x', 'y'])
                        .agg({'AIN':'count'})
                        .reset_index()
                        .rename(columns = {'AIN':'num_AIN'})
    )

    # Create a uuid for all the AINs that have same lat/lon for centroids.
    duplicate_geom['uuid'] = [str(uuid.uuid4()) for x in range(len(duplicate_geom.index))]

    # Create a uuid for all the AINs that have same lat/lon for centroids.
    duplicate_geom['uuid'] = [str(uuid.uuid4()) for x in range(len(duplicate_geom.index))]

    parcels2 = pd.merge(parcels, duplicate_geom, 
                    on = ['x', 'y'], how = 'left', validate = 'm:1')
    
    print(list(parcels2.columns))
    
    parcels2.to_parquet(
        f's3://{bucket_name}/gis/intermediate/la_parcels_with_dups.parquet')
    
    # Count total number of parcels within tract
    counts_by_tract = (parcels2.drop_duplicates(subset = ["uuid", "GEOID"], 
                        keep = "first")
                        .groupby("GEOID")
                        .agg({"AIN":"count"})
                        .reset_index()
                        .rename(columns = {"AIN": "total_AIN"})
    )

    parcels3 = pd.merge(parcels2, counts_by_tract, on = "GEOID", 
                how = "left", validate = "m:1")    
    
    integrify_me = ["num_AIN", "total_AIN"]
    parcels3[integrify_me] = parcels3[integrify_me].astype("Int64")

    stringme = ["AIN"]
    parcels3[stringme] = parcels3[stringme].astype("str")

    print(list(parcels3.columns))
    
    parcels3 = parcels3[parcels3.GEOID.notna()]
    parcels3.to_parquet(
        f's3://{bucket_name}/gis/intermediate/la_parcels_with_dups2.parquet')

    return parcels3

'''
#------------------------------------------------------------------------#
## Add TOC-eligibility columns to crosswalk_parcels_tracts
#------------------------------------------------------------------------#
def tag_toc_eligible_tracts(crosswalk_parcels_tracts):
    # Import full list of toc_parcels
    toc_parcels = (gpd.read_file(
        f'zip+s3://{bucket_name}/gis/intermediate/TOC_Parcels.zip')
        [["AIN", "TOC_Tier"]]
    )

    # Merge onto crosswalk
    crosswalk_parcels_tracts = pd.merge(crosswalk_parcels_tracts, 
            toc_parcels[toc_parcels.TOC_Tier > 0][["AIN", "TOC_Tier"]], 
            on = "AIN", how = "left", validate = "1:1"
    )
    
    print(list(parcels.columns))
    
     # There are 423 unique AIN that are not linked to a tract GEOID
    # Get rid of this right before the crosswalk is overwritten
    parcels2 = pd.DataFrame(
            (parcels[parcels.GEOID.notna()]
            .drop(columns = ["geometry", "index_right"])
            )
    )
    parcels2.to_parquet(
        f's3://{bucket_name}/data/crosswalk_parcels_tracts.parquet')

    return parcels2

    # Make sure we capture all the historical AINs and TOC Tiers
    crosswalk_parcels_tracts = crosswalk_parcels_tracts.assign(
        TOC_Tier = (crosswalk_parcels_tracts.groupby("uuid")["TOC_Tier"]
                    .transform("max")
                    .fillna(0).astype(int)
        )
    )

    # Get rid of duplicate AIN's
    def in_tier(row):
        if row.TOC_Tier != 0:
            return 1
        else:
            return 0

    df = (crosswalk_parcels_tracts.sort_values(["uuid", "TOC_Tier"], 
            ascending = [True, False])
        .drop_duplicates(subset = "uuid", keep = "first")
        .reset_index(drop=True)
        )
    
    # Tag if the parcel counts as in TOC tier or not
    df = df.assign(
        in_tier = df.apply(lambda x: 1 if x.TOC_Tier != 0 else 0, axis=1)
    )


    # Aggregate by in_tier 
    df2 = (df.groupby(["GEOID", "total_AIN", "in_tier"])
          .agg({"num_AIN": "count"})
          .reset_index()
         )
    
    # If GEOID has 2 observations, one in_tier==1 and other in_tier==0, 
    # let's keep the in_tier==1
    df2["obs"] = df2.groupby("GEOID").cumcount() + 1
    df2["max_obs"] = df2.groupby("GEOID")["obs"].transform("max")
    
    df3 = (df2[(df2.max_obs == 1) | 
             ((df2.in_tier == 1) & (df2.max_obs == 2))]
          .drop(columns = ["obs", "max_obs"])
         )
        
    # Calculate the % of AIN that falls within TOC tiers    
    df3 = (df3.assign(
            pct_toc_AIN = df3.apply(
                lambda x: x.num_AIN / x.total_AIN if x.in_tier==1
                else 0, axis=1)
        ).sort_values("GEOID")
        .reset_index(drop=True)
    )

    df4 = df3.assign(
        toc_AIN = df3.apply(lambda x: 1 if (
                            (x.in_tier == 1) and (x.pct_toc_AIN >= 0.5)
                            ) 
                            else 0, axis=1)
    )
    
    print(list(df4.columns))

    # Merge these new TOC columns back onto original crosswalk
    df5 = pd.merge(crosswalk_parcels_tracts, 
                df4[["GEOID", "pct_toc_AIN", "toc_AIN"]], 
                on = "GEOID", how = "left", validate = "m:1")
    
    print(list(df5.columns))

    keep_cols = ["uuid", "AIN",  "x", "y", "num_AIN", "TOC_Tier", 
                "GEOID", "total_AIN", "pct_toc_AIN", "toc_AIN"]

    df5[keep_cols].to_parquet(
        f's3://{bucket_name}/data/crosswalk_parcels_tracts.parquet')
    
    return df5


gdf = tag_duplicate_parcels()
final = tag_toc_eligible_tracts(gdf)
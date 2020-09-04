"""
Script to load parcel data from LA County and join it with Census tracts.

We choose parcel data that combines rolls for the past ~10 years in order
to get a more maximal view of AINs that have existed in the parcel.
For parcels that exist in more than one year (most of them), we choose
the most recent year.

Relies on a locally downloaded version of the 14GB CSV here:
https://data.lacounty.gov/Parcel-/Assessor-Parcels-Data-2006-thru-2019/9trm-uz8i
"""
import dask.dataframe as dd
import intake_dcat
import geopandas
import pandas

CHUNK = 1_000_000
bucket_name = "city-planning-entitlements"

def main(county_parcel_path):
    # Normally we would paralellize using something like
    # dask, or go out of core with something like vaex,
    # but they both failed on the relevant CSV due to 
    # line ending weirdness. Instead we iteratively build
    # up the parcels file.
    reader = pandas.read_csv(
        county_parcel_path,
        iterator=True,
        chunksize=CHUNK,
        dtype={
            'PropertyUseCode': 'object',
            'ZIPcode5': 'object',
            'AdministrativeRegion': 'object',
        },
    )
    i = 0
    for chunk in reader:
        print(f"Reading chunk {i}")
        cols = [c for c in chunk.columns if c not in ["RollYear", "AIN"]]
        agg = chunk.groupby("AIN").agg({
            "RollYear": "max",
            **{c: "first" for c in cols},
        })
        agg.to_parquet(f"parcels_{i}.parquet")
        i = i + 1

    # Use Dask to combine the resulting parcels subfiles
    print("Combining sub-parquets")
    ddf = dd.read_parquet("parcels_*.parquet", engine="pyarrow")
    cols = [c for c in ddf.columns if c not in ["RollYear", "AIN"]]
    agg = ddf.groupby("AIN").agg({
        "RollYear": "max",
        **{c: "first" for c in cols},
    })
    df = agg.compute()
    df.to_parquet(f"s3://{bucket_name}/data/source/Assessor_Parcels_Data_2006_2019.parquet")
    
    # Load census tract data from the county
    print("Downloading tract data")
    lacounty_data = intake_dcat.DCATCatalog("https://data.lacounty.gov/data.json")
    tracts = lacounty_data["https://data.lacounty.gov/api/views/ay2y-b9rg"].read()

    # Join the datasets
    print("Joining to tract data")
    gdf = geopandas.GeoDataFrame(
        df,
        geometry=geopandas.points_from_xy(df.CENTER_LON, df.CENTER_LAT),
        crs="EPSG:4326",
    )
    joined = geopandas.sjoin(
        gdf,
        tracts.rename(columns={"geoid10": "GEOID"})[["GEOID", "geometry"]],
        op="within",
        how="left",
    ).drop(columns=["index_right"])

    return joined
    

if __name__ == "__main__":
    import sys
    import s3fs
    
    df = main(sys.argv[1])
    print("Uploading file to s3")
    fs = s3fs.S3FileSystem()
    df.to_parquet(f"s3://{bucket_name}/gis/intermediate/lacounty_parcels.parquet", 
            filesystem=fs)
# Utils for src folder
import boto3
import geopandas as gpd
import os
import pandas as pd
import shapely
import shutil

from shapely.geometry import Point

s3 = boto3.client("s3")
bucket_name = "city-planning-entitlements"


# Add geometry column, then convert df to gdf
def make_gdf(df, x_col, y_col, initial_CRS="EPSG:4326", projected_CRS="EPSG:2229"):
    # Some of the points will throw up errors when creating geometry
    df = df.dropna(subset=[x_col, y_col])
    df = df[(df[x_col] != 0) & (df[y_col] != 0)]
    # Make geometry
    df = (df.assign(
            geometry = df.apply(lambda row: Point(row[x_col], row[y_col]), axis=1)
        ).drop(columns = [x_col, y_col])
    )
    
    # Convert to gdf
    gdf = gpd.GeoDataFrame(df)
    gdf.crs = initial_CRS
    gdf = (gdf[df.geometry.notna()]
            .set_geometry('geometry')
        )
    gdf = gdf.to_crs(projected_CRS)
    return gdf


# Make zipped shapefile
# Remember: shapefiles can only take 10-char column names
def make_zipped_shapefile(df, path):
    """
    Make a zipped shapefile and save locally

    Parameters
    ==========

    df: gpd.GeoDataFrame to be saved as zipped shapefile
    path: str, local path to where the zipped shapefile is saved.
            Ex: "folder_name/census_tracts" 
                "folder_name/census_tracts.zip"
    """

    # Grab first element of path (can input filename.zip or filename)
    dirname = os.path.splitext(path)[0]
    print(f'Path name: {path}')
    print(f'Dirname (1st element of path): {dirname}')
    # Make sure there's no folder with the same name
    shutil.rmtree(dirname, ignore_errors = True)
    # Make folder
    os.mkdir(dirname)
    shapefile_name = f'{os.path.basename(dirname)}.shp'
    print(f'Shapefile name: {shapefile_name}')
    # Export shapefile into its own folder with the same name 
    df.to_file(driver = 'ESRI Shapefile', filename = f'{dirname}/{shapefile_name}')
    print(f'Shapefile component parts folder: {dirname}/{shapefile_name}')
    # Zip it up
    shutil.make_archive(dirname, 'zip', dirname)
    # Remove the unzipped folder
    shutil.rmtree(dirname, ignore_errors = True)


# Upload S3 geoparquet
def upload_geoparquet(gdf, file_name="my_file.parquet", 
            bucket_name = "city-planning-entitlements", 
            local_path="", S3_path=""):
    
    """
    Save GeoDataFrame as geoparquet locally,
    uploads to S3, and removes local version.

    geopandas>=0.8.0 supports initial geoparquets.

    Parameters
    ==========

    gdf: gpd.GeoDataFrame to be saved as geoparquet
    file_name: str, name of the file, such as "census_tracts.parquet"
    bucket_name: str, S3 bucket name.
    local_path: str, the local directory or folder path where the file is stored locally.
                Ex: "./data/"
    S3_path: str, the S3 directory or folder path to where the file should be stored in S3.
            Ex: "data/"
    """    
    gdf.to_parquet(f'{local_path}{file_name}')

    s3.upload_file(f'{local_path}{file_name}', bucket_name, f'{S3_path}{file_name}')
    os.remove(f'{local_path}{file_name}')


# Download S3 geoparquet and import
def download_geoparquet(file_name="my_file.parquet", 
            bucket_name = "city-planning-entitlements", 
            local_path="", S3_path=""):
    
    """
    Downloads geoparquet from S3 locally,
    read into memory as GeoDataFrame, and removes local version.

    geopandas>=0.8.0 supports initial geoparquets.

    Parameters
    ==========

    file_name: str, name of the file, such as "census_tracts.parquet"
    bucket_name: str, S3 bucket name.
    local_path: str, the local directory or folder path where the file should be stored.
                Ex: "./data/"
    S3_path: str, the S3 directory or folder path to where the file is stored in S3.
            Ex: "data/"
    """ 
    s3.download_file(bucket_name, f'{S3_path}{file_name}', f'{local_path}{file_name}')
    gdf = gpd.read_parquet(f'{local_path}{file_name}')
    os.remove(f'{local_path}{file_name}')
    
    return gdf


# Reconstruct toc_tiers file, which has multiple geometry columns.
# Multiple geojsons are saved, each geojson with just 1 geometry column.
def reconstruct_toc_tiers_file(**kwargs):
    dataframes = {}
    for i in range(0, 5):
        df = gpd.read_file(
            f's3://{bucket_name}/gis/intermediate/reconstructed_toc_tiers_{i}.geojson')
        key = f'tier{i}'
        new_geometry_col = f'tier_{i}'
        if i == 0:
            dataframes[key] = df
        if i > 0:
            df.rename(columns = {'geometry': new_geometry_col}, inplace = True)
            df = df.set_geometry(new_geometry_col)
            dataframes[key] = df[["tiers_id", new_geometry_col]] 
        
    toc_tiers = pd.merge(dataframes["tier0"], dataframes["tier1"], 
                         on = "tiers_id", how = "left", validate = "1:1")
    for i in range(2, 5):
        new_key = f"tier{i}"
        toc_tiers = pd.merge(toc_tiers, dataframes[new_key], 
                             on = "tiers_id", how = "left", validate = "1:1")    
    
    col_order = [
        "tiers_id", "line_id_a", "line_id_b", "line_name_a", "line_name_b", "station_id", "station_name",
        "geometry", "tier_1", "tier_2", "tier_3", "tier_4",
        "mode_a", "mode_b", "agency_a", "agency_b"
    ]  

    # Fill in Nones in geometry columns with GeometryColumnEmpty
    for col in ["tier_1", "tier_2", "tier_3", "tier_4"]:
        toc_tiers[col] = toc_tiers.apply(
            lambda row: shapely.geometry.GeometryCollection() if row[col] is None
            else row[col], axis = 1)
        toc_tiers = toc_tiers.set_geometry(col)
    
    toc_tiers = toc_tiers.set_geometry('geometry')
    
    return toc_tiers[col_order]
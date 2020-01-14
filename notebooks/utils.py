# Utils for src/roadway_design/ folder
import os
import shutil
from shapely.geometry import Point
import geopandas as gpd


# Add geometry column, then convert df to gdf
def make_gdf(df):
    # Some of the points will throw up errors when creating geometry
    df = df.dropna(subset=['CENTER_LAT', 'CENTER_LON'])
    df = df[(df.CENTER_LAT != 0) & (df.CENTER_LAT != 0)]
    # Make geometry
    df['geometry'] = df.apply(
        lambda row: Point(row.CENTER_LON, row.CENTER_LAT), axis=1)
    df.rename(columns = {'point_x': 'lon', 'point_y':'lat'}, inplace=True)
    # Convert to gdf
    gdf = gpd.GeoDataFrame(df)
    gdf.crs = {'init':'epsg:4326'}
    gdf = gdf[df.geometry.notna()]
    gdf = gdf.to_crs({'init':'epsg:2229'})
    return gdf


# Make zipped shapefile (used for AGOL web app)
def make_zipped_shapefile(df, path):
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
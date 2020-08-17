# Utils for notebooks folder
import dataclasses
import os
import re
import shutil
import typing
from shapely.geometry import Point
import shapely
import geopandas as gpd
import numpy as np
import pandas as pd
import toc

bucket_name = "city-planning-entitlements"

# Add geometry column, then convert df to gdf
def make_gdf(df, x_col, y_col):
    # Some of the points will throw up errors when creating geometry
    df = df.dropna(subset=[x_col, y_col])
    df = df[(df.x_col != 0) & (df.y_col != 0)]
    # Make geometry
    df['geometry'] = df.apply(
        lambda row: Point(row[x_col], row[y_col]), axis=1)
    df.rename(columns = {'point_x': 'lon', 'point_y':'lat'}, inplace=True)
    # Convert to gdf
    gdf = gpd.GeoDataFrame(df)
    gdf.crs = 'EPSG:4326'
    gdf = gdf[df.geometry.notna()]
    gdf = gdf.to_crs('EPSG:2229')
    return gdf


# Make zipped shapefile
# Remember: shapefiles can only take 10-char column names
def make_zipped_shapefile(df, path):
    # Grab first element of path (can input filename.zip or filename)
    dirname = os.path.splitext(path)[0]
    print(f"Path name: {path}")
    print(f"Dirname (1st element of path): {dirname}")
    # Make sure there's no folder with the same name
    shutil.rmtree(dirname, ignore_errors=True)
    # Make folder
    os.mkdir(dirname)
    shapefile_name = f"{os.path.basename(dirname)}.shp"
    print(f"Shapefile name: {shapefile_name}")
    # Export shapefile into its own folder with the same name
    df.to_file(driver="ESRI Shapefile", filename=f"{dirname}/{shapefile_name}")
    print(f"Shapefile component parts folder: {dirname}/{shapefile_name}")
    # Zip it up
    shutil.make_archive(dirname, "zip", dirname)
    # Remove the unzipped folder
    shutil.rmtree(dirname, ignore_errors=True)


def get_centroid(parcels):
    parcels['centroid'] = parcels.geometry.centroid
    parcels2 = parcels.set_geometry('centroid')
    parcels2 = parcels[['AIN', 'TOC_Tier', 'centroid']]
    # Get the X, Y points from centroid, because we can use floats (but not geometry) to determine duplicates
    # In a function, doing parcels.centroid.x doesn't work, but mapping it does work.
    parcels2['x'] = parcels2.centroid.map(lambda row: row.x)
    parcels2['y'] = parcels2.centroid.map(lambda row: row.y)
    # Count number of obs that have same centroid
    parcels2['obs'] = parcels2.groupby(['x', 'y']).cumcount() + 1
    parcels2['num_obs'] = parcels2.groupby(['x', 'y'])['obs'].transform('max')
    # Convert to gdf
    parcels2 = gpd.GeoDataFrame(parcels2).rename(columns = {'centroid':'geometry'})
    parcels2.crs = 'EPSG:2229'
    parcels2 = parcels2.rename(columns = {'geometry':'centroid'}).set_geometry('centroid')
    return parcels2


#---------------------------------------------------------------------------------------#
## Other functions
#---------------------------------------------------------------------------------------# 
# Reconstruct toc_tiers file, which has multiple geometry columns.
# Multiple geojsons are saved, each geojson with just 1 geometry column.
def reconstruct_toc_tiers_file(**kwargs):
    dataframes = {}
    for i in range(0, 5):
        df = gpd.read_file(f's3://city-planning-entitlements/gis/intermediate/reconstructed_toc_tiers_{i}.geojson')
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
        toc_tiers[col] = toc_tiers.apply(lambda row: shapely.geometry.GeometryCollection() if row[col] is None
                                         else row[col], axis = 1)
        toc_tiers = toc_tiers.set_geometry(col)
    
    toc_tiers = toc_tiers.set_geometry('geometry')
    
    return toc_tiers[col_order]


# Reconstruct joining the parcels to the toc_tiers file.
# This is flexible, we can subset gdf to be num_TOC > 0 or not.
def parcels_join_toc_tiers(gdf, toc_tiers):
    """ 
    gdf: gpd.GeoDataFrame
        The parcel-level df with the number of entitlements attached.
    toc_tiers: gpd.GeoDataFrame
        The buffers around each bus/rail intersection drawn for each tier.
    """
    tier_1 = toc.join_with_toc_tiers(gdf[gdf.TOC_Tier==1], toc_tiers, 1)
    tier_2 = toc.join_with_toc_tiers(gdf[gdf.TOC_Tier==2], toc_tiers, 2)
    tier_3 = toc.join_with_toc_tiers(gdf[gdf.TOC_Tier==3], toc_tiers, 3)
    tier_4 = toc.join_with_toc_tiers(gdf[gdf.TOC_Tier==4], toc_tiers, 4)
    
    tier_3 = tier_3.assign(
        a_rapid = tier_3.apply(lambda x: toc.is_rapid_bus2(x.agency_a, x.line_name_a, x.mode_a), axis=1),
        b_rapid = tier_3.apply(lambda x: toc.is_rapid_bus2(x.agency_b, x.line_name_b, x.mode_b), axis=1),
    )
    tier_2 = tier_2.assign(
        a_rapid = tier_2.apply(lambda x: toc.is_rapid_bus2(x.agency_a, x.line_name_a, x.mode_a), axis=1),
        b_rapid = tier_2.apply(lambda x: toc.is_rapid_bus2(x.agency_b, x.line_name_b, x.mode_b), axis=1),
    )
    tier_1 = tier_1.assign(
        a_rapid = tier_1.apply(lambda x: toc.is_rapid_bus2(x.agency_a, x.line_name_a, x.mode_a), axis=1),
        b_rapid = tier_1.apply(lambda x: toc.is_rapid_bus2(x.agency_b, x.line_name_b, x.mode_b), axis=1),
    )
    
    col_order = [
        'AIN', 'TOC_Tier', 'zone_class', 'num_TOC', 'num_nonTOC', 'geometry',
        'tiers_id', 'line_id_a', 'line_id_b', 'line_name_a', 'line_name_b',
        'station_id', 'station_name', 'tier_1', 'tier_2', 'tier_3', 'tier_4', 
        'mode_a', 'mode_b', 'agency_a', 'agency_b', 'a_rapid', 'b_rapid',
    ]
    
    df = pd.concat([
        tier_1,
        tier_2,
        tier_3,
        tier_4
    ], sort = False).reset_index(drop = True).reindex(columns = col_order)
    
    # Fill in Nones in geometry columns with GeometryColumnEmpty
    for col in ["tier_1", "tier_2", "tier_3", "tier_4"]:
        df = df.set_geometry(col)
        df[col] = df.apply(lambda row: 
                           shapely.geometry.GeometryCollection() if row[col] is None 
                           else row[col], axis = 1)
        df = df.set_geometry(col)
    
    return df

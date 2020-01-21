# Utils for src/roadway_design/ folder
import dataclasses
import os
import re
import shutil
import typing
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


# A regex for parsing a zoning string
ZONE_RE = re.compile("^(\(T\)|\[T\]|T|\(Q\)|\[Q\]|Q)?(\(T\)|\[T\]|T|\(Q\)|\[Q\]|Q)?([A-Z0-9.]+)-([A-Z0-9]+)((?:-[A-Z]+)*)$")
# The different forms that the T/Q zoning prefixes may take.
T_OPTIONS = {'T', '(T)', '[T]'}
Q_OPTIONS = {'Q', '(Q)', '[Q]'}

VALID_ZONE_CLASS = {
    'A1', 'A2', 'RA',
    'RE', 'RE40', 'RE20', 'RE15', 'RE11', 'RE9',
    'RS', 'R1', 'R1V', 'R1F', 'R1R', 'R1H', 'RU', 'RZ2.5', 'RZ3', 'RZ4', 'RW1',
    'R2', 'RD1.5', 'RD2', 'RD3', 'RD4', 'RD5', 'RD6', 'RMP', 'RW2', 'R3', 'RAS3', 'R4', 'RAS4', 'R5',
    'CR', 'C1', 'C1.5', 'C2', 'C4', 'C5', 'CM',
    'MR1', 'M1', 'MR2', 'M2', 'M3',
    'P', 'PB', 'OS', 'PF', 'SL'
}

VALID_HEIGHT_DISTRICTS = {
    '1', '1D', 
    '1L', '1LD', 
    '1VL', '1VLD', 
    '1XL', '1XLD', 
    '1SS', '1SSD', 
    '2', '2D', 
    '3', '3D', 
    '4', '4D', 
}

INVALID_HEIGHT_DISTRICTS = {'1A', '1B'}


@dataclasses.dataclass
class ZoningInfo:
    """
    A dataclass for parsing and storing parcel zoning info.
    The information is accessible as data attributes on the class instance.
    If the constructor is unable to parse the zoning string,
    a ValueError will be raised.

    References
    ==========

    https://planning.lacity.org/zoning/guide-current-zoning-string
    https://planning.lacity.org/odocument/eadcb225-a16b-4ce6-bc94-c915408c2b04/Zoning_Code_Summary.pdf
    """
    Q: bool = False
    T: bool = False
    zone_class: str = ""
    D: bool = False
    height_district: str = ""
    overlay: typing.Optional[typing.List[str]] = None


    def __init__(self, zoning_string: str):
        """
        Create a new ZoningInfo instance.
        
        Parameters
        ==========

        zoning_string: str
            The zoning string to be parsed.
        """
        matches = ZONE_RE.match(zoning_string)
        if matches is None:
            raise ValueError("Couldn't parse zoning string")
        
        groups = matches.groups()
        
        # Prefix
        if groups[0] in T_OPTIONS or groups[1] in T_OPTIONS:
            self.T = True
        
        if groups[0] in Q_OPTIONS or groups[1] in Q_OPTIONS:
            self.Q = True
        
        # Zone Class
        zone_class = groups[2] or ""

        # Check for valid zone class values
        if zone_class not in VALID_ZONE_CLASS:
            raise ValueError("Invalid zone class")

        self.zone_class = zone_class

        # Height District
        height_district = groups[3] or ""
        
        # D Limit
        if height_district[-1] == "D":
            self.D = True
            height_district = height_district[:-1]
        else:
            self.D = False
            height_district = height_district
        
        # Check for valid height district values
        if height_district not in VALID_HEIGHT_DISTRICTS or height_district in INVALID_HEIGHT_DISTRICTS:
            raise ValueError("Invalid height district")
        
        self.height_district = height_district

        # Overlays
        if groups[4]:
            self.overlay = groups[4].strip('-').split('-')
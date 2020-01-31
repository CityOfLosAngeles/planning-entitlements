# Utils for notebooks folder
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
    df = df.dropna(subset=["CENTER_LAT", "CENTER_LON"])
    df = df[(df.CENTER_LAT != 0) & (df.CENTER_LAT != 0)]
    # Make geometry
    df["geometry"] = df.apply(lambda row: Point(row.CENTER_LON, row.CENTER_LAT), axis=1)
    df.rename(columns={"point_x": "lon", "point_y": "lat"}, inplace=True)
    # Convert to gdf
    gdf = gpd.GeoDataFrame(df)
    gdf.crs = {"init": "epsg:4326"}
    gdf = gdf[df.geometry.notna()]
    gdf = gdf.to_crs({"init": "epsg:2229"})
    return gdf


# Make zipped shapefile (used for AGOL web app)
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


#---------------------------------------------------------------------------------------#
## Parser
#---------------------------------------------------------------------------------------#
VALID_ZONE_CLASS = {
    "A1",
    "A2",
    "RA",
    "RE",
    "RE40",
    "RE20",
    "RE15",
    "RE11",
    "RE9",
    "RS",
    "R1",
    "R1F",
    "R1R",
    "R1H",
    "RU",
    "RZ2.5",
    "RZ3",
    "RZ4",
    "RW1",
    "R2",
    "RD1.5",
    "RD2",
    "RD3",
    "RD4",
    "RD5",
    "RD6",
    "RMP",
    "RW2",
    "R3",
    "RAS3",
    "R4",
    "RAS4",
    "R5",
    # Additional residential ones from master
    "R1R3",
    "R1H1",
    "R1V1",
    "R1V2",
    "R1V3",
    "CR",
    "C1",
    "C1.5",
    "C2",
    "C4",
    "C5",
    "CM",
    "MR1",
    "M1",
    "MR2",
    "M2",
    "M3",
    "P",
    "PB",
    # Additional parking ones from master
    "R1P",
    "R2P",
    "R3P",
    "R4P",
    "R5P",
    "RAP",
    "RSP",
    "OS",
    "GW",
    "PF",
    "FRWY",
    "SL",
    # Hybrid Industrial
    "HJ",
    "HR",
    "NI",
}

VALID_HEIGHT_DISTRICTS = {
    "1",
    "1L",
    "1VL",
    "1XL",
    "1SS",
    "2",
    "3",
    "4",
}

VALID_SUPPLEMENTAL_USE = {
    # Supplemental Use found in Table 2 or Zoning Code Article 3
    "O",
    "S",
    "G",
    "K",
    "CA",
    "MU",
    "FH",
    "SN",
    "HS",
    "RG",
    "RPD",
    "POD",
    "CDO",
    "NSO",
    "RFA",
    "MPR",
    "RIO",
    "HCR",
    "CPIO",
    "CUGU",
    "HPOZ",
    "SP",
    # Add more from their master table
    "NMU",
}

# Valid specific plans.
# TODO: handle Warner Center and UCS zones specifically,
# since they don't match the pattern of the rest of the specific plans.
VALID_SPECIFIC_PLAN = {
    # Found in Zoning Code Article 2 and Sec 12.04 Zones-Districts-Symbols.
    "CEC",
    "CW",
    "GM",
    "OX",
    "PV",
    "WC",
    "ADP",
    "CCS",
    "CSA",
    "PKM",
    "LAX",
    "LASED",
    #"USC-1A",
    #"USC-1B",
    #"USC-2",
    #"USC-3",
    "PVSP",
    # Add more from their master table
    #"(WC)COLLEGE",
    #"(WC)COMMERCE",
    #"(WC)DOWNTOWN",
    #"(WC)NORTHVILLAGE",
    #"(WC)PARK",
    #"(WC)RIVER",
    #"(WC)TOPANGA",
    #"(WC)UPTOWN",
    "UV",
    "PKM",
    "EC",
    "PPSP",
}

# Regex for qualified condition or tentative classifications
Q_T_RE = "(\(T\)|\[T\]|T|\(Q\)|\[Q\]|Q)?(\(T\)|\[T\]|T|\(Q\)|\[Q\]|Q)?"
# Specific plan
SPECIFIC_PLAN_RE = "(?:\(([A-Z]+)\))?"
# Zoning class
ZONING_CLASS_RE = "([A-Z0-9.]+)"
# Height district
HEIGHT_DISTRICT_RE = "([A-Z0-9]+)"
# Overlays
OVERLAY_RE = "((?:-[A-Z]+)*)"

# A regex for parsing a zoning string
FULL_ZONE_RE = re.compile(
    f"^{Q_T_RE}{SPECIFIC_PLAN_RE}{ZONING_CLASS_RE}{SPECIFIC_PLAN_RE}-{HEIGHT_DISTRICT_RE}{OVERLAY_RE}$"
)

# Zoning class only regex, for cases where height district and overlays may be missing
ZONE_ONLY_RE = re.compile(
    f"^{Q_T_RE}{SPECIFIC_PLAN_RE}{ZONING_CLASS_RE}{SPECIFIC_PLAN_RE}$"
)

# The different forms that the T/Q zoning prefixes may take.
T_OPTIONS = {"T", "(T)", "[T]"}
Q_OPTIONS = {"Q", "(Q)", "[Q]"}


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
    specific_plan: str = ""
    overlay: typing.List[str] = dataclasses.field(default_factory=list)

    def __init__(self, zoning_string: str):
        """
        Create a new ZoningInfo instance.
        
        Parameters
        ==========

        zoning_string: str
            The zoning string to be parsed.
        """
        try:
            self._parse_full(zoning_string)
        except ValueError:
            self._fallback(zoning_string)

    def _parse_full(self, zoning_string: str):
        matches = FULL_ZONE_RE.match(zoning_string.strip())
        if matches is None:
            raise ValueError(f"Couldn't parse zoning string {zoning_string}")
        groups = matches.groups()
        
        # Prefix
        if groups[0] in T_OPTIONS or groups[1] in T_OPTIONS:
            self.T = True
        
        if groups[0] in Q_OPTIONS or groups[1] in Q_OPTIONS:
            self.Q = True

        self.specific_plan = groups[2] or groups[4] or ""
        self.zone_class = groups[3] or ""
        height_district = groups[5] or ""
        if height_district[-1] == "D":
            self.D = True
            height_district = height_district[:-1]
        else:
            self.D = False
            self.height_district = height_district
        if groups[6]:
            self.overlay = groups[6].strip("-").split("-")
        else:
            self.overlay = []

        self._validate()

    def _validate(self):
        try:
            assert self.zone_class in VALID_ZONE_CLASS
            assert (
                self.height_district in VALID_HEIGHT_DISTRICTS or self.height_district
            )
            if self.overlay:
                assert all([o in VALID_SUPPLEMENTAL_USE for o in self.overlay])
            assert self.specific_plan in VALID_SPECIFIC_PLAN or self.specific_plan is ""
        except AssertionError:
            raise ValueError(f"Failed to validate")

    def _fallback(self, zoning_string: str):
        # Brute force the parts of the string, trying to assign them on a
        # best-effort basis.
        self.overlay = []
        parts = zoning_string.split("-")
        for part in parts:
            # Since the ZONE_ONLY_RE should also match height district or overlay
            # components, we match each part of the zoning string against that.
            # This allows us to check for Q/T and specific plan conditions.
            match = ZONE_ONLY_RE.match(part.strip())
            if not match:
                raise ValueError(f"Couldn't parse zoning string {zoning_string}")
            groups = match.groups()
            if groups[0] in T_OPTIONS or groups[1] in T_OPTIONS:
                self.T = True
            if groups[0] in Q_OPTIONS or groups[1] in Q_OPTIONS:
                self.Q = True
            for g in groups[2:]:
                if g is None:
                    continue
                if g in VALID_SPECIFIC_PLAN:
                    self.specific_plan = g
                elif g in VALID_ZONE_CLASS:
                    self.zone_class = g
                elif g in VALID_HEIGHT_DISTRICTS:
                    self.height_district = g
                    self.D = False
                elif g[:-1] in VALID_HEIGHT_DISTRICTS and g[-1] == "D":
                    self.D = True
                    self.height_district = g[:-1]
                elif g in VALID_SUPPLEMENTAL_USE:
                    self.overlay.append(g)
                else:
                    raise ValueError(
                        f"Couldn't parse zoning string {zoning_string}, component {g}"
                    )

"""
Utility functions for analyzing Transit Oriented Communities (TOC)
entitlements, following Measure JJJ.
"""
import datetime
import os
import typing

import fsspec
import geopandas
import pandas
import partridge
import shapely


GTFS_FILE = os.path.join("/tmp", "gtfs.zip")
TEST_DATE = datetime.date(2020, 2, 18)

WGS84 = 4326
SOCAL_FEET = 2229

# We can relax TOC rules a bit to catch more edge cases
# upon assigning entitlements to a given geometry. Additionally,
# we are likely getting the position of stops and stations slightly
# wrong, so this helps make that a bit less of a problem.
# This corresponds to the factor by which we increase buffers.
DEFAULT_CUSHION = 1.2


def bus_peak_frequencies(
    gtfs_path: str,
    test_date: typing.Optional[datetime.date] = None,
    am_peak: typing.Optional[typing.Tuple[int, int]] = None,
    pm_peak: typing.Optional[typing.Tuple[int, int]] = None,
) -> geopandas.GeoDataFrame:
    """
    Compute AM and PM Peak frequencies for all the lines in a GTFS Feed.

    Parameters
    ==========
    gtfs_path: str
        The path (or URL) to a GTFS feed.
    test_date: datetime.date
        The test date for which to compute frequencies. Defaults to February
        18th, 2020, an unremarkable weekday February.
    am_peak: tuple of integers
        The two hours (out of 24) demarcating the AM peak period.
    pm_peak: tuple of integers
        The two hours (out of 24) demarcating the PM peak period.
    """

    # Set default values
    test_date = test_date or TEST_DATE
    am_peak = am_peak or (6, 9)
    pm_peak = pm_peak or (15, 19)

    am_duration = am_peak[1] - am_peak[0]
    pm_duration = pm_peak[1] - pm_peak[0]

    assert am_duration > 0
    assert pm_duration > 0

    # Download and read the GTFS feed
    with fsspec.open(gtfs_path, "rb") as infile:
        data = infile.read()
    with open(GTFS_FILE, "wb") as outfile:
        outfile.write(data)
    service_by_date = partridge.read_service_ids_by_date(GTFS_FILE)
    feed = partridge.load_geo_feed(GTFS_FILE)

    # Get the service for the test date
    try:
        test_service = next(v for k, v in service_by_date.items() if k == test_date)
    except StopIteration:
        raise ValueError(f"Could not find service for {test_date}")

    test_trips = feed.trips[feed.trips.service_id.isin(test_service)]
    test_stops = feed.stop_times[feed.stop_times.trip_id.isin(test_trips.trip_id)]

    # Get the departure, arrival, and mean time for each trip
    trip_timings = test_stops.groupby(test_stops.trip_id).agg(
        {"departure_time": min, "arrival_time": max}
    )
    trip_timings = trip_timings.assign(
        mean_time=trip_timings.departure_time
        + (trip_timings.arrival_time - trip_timings.departure_time) / 2.0
    )

    # Find all of the trips that fall within the AM and PM peak times.
    am_peak_trips = trip_timings[
        (trip_timings.mean_time > am_peak[0] * 60 * 60)
        & (trip_timings.mean_time < am_peak[1] * 60 * 60)
    ]
    pm_peak_trips = trip_timings[
        (trip_timings.mean_time > pm_peak[0] * 60 * 60)
        & (trip_timings.mean_time < pm_peak[1] * 60 * 60)
    ]
    am_peak_trips = test_trips.merge(
        am_peak_trips, left_on=test_trips.trip_id, right_index=True,
    )
    pm_peak_trips = test_trips.merge(
        pm_peak_trips, left_on=test_trips.trip_id, right_index=True,
    )

    # Compute the peak frequency
    am_peak_frequency = (
        am_peak_trips.groupby([am_peak_trips.route_id, am_peak_trips.direction_id])
        .size()
        .to_frame("am_peak_trips")
    )
    am_peak_frequency = am_peak_frequency.assign(
        am_peak_frequency=am_duration * 60 / am_peak_frequency.am_peak_trips
    )
    pm_peak_frequency = (
        pm_peak_trips.groupby([pm_peak_trips.route_id, pm_peak_trips.direction_id])
        .size()
        .to_frame("pm_peak_trips")
    )
    pm_peak_frequency = pm_peak_frequency.assign(
        pm_peak_frequency=pm_duration * 60 / pm_peak_frequency.pm_peak_trips
    )
    peak_frequency = pandas.concat(
        [am_peak_frequency, pm_peak_frequency], axis=1, sort=False
    )

    # Add the route short name for easier legibility.
    peak_frequency = peak_frequency.join(
        feed.routes[["route_id", "route_short_name"]].set_index("route_id"),
        how="left",
        on="route_id",
    )

    # Grab the most popular shape as the official one.
    route_shapes = (
        test_trips.groupby("route_id")
        .agg({"shape_id": lambda s: s.value_counts().index[0]})
        .reset_index()
        .merge(feed.shapes, how="left", on="shape_id")
        .set_index("route_id")
        .drop(columns=["shape_id"])
    )

    peak_frequency = peak_frequency.merge(
        route_shapes, how="left", right_index=True, left_index=True
    ).assign(agency=feed.agency.agency_name.iloc[0])

    gdf = geopandas.GeoDataFrame(peak_frequency, geometry="geometry")
    gdf.crs = {"init": f"epsg:{WGS84}"}
    return gdf


def toc_bus_lines(
    gtfs_path: str, cutoff: float = 15.0, **kwargs
) -> geopandas.GeoDataFrame:
    """
    Get the lines qualifying for TOC for a given GTFS path.

    Parameters
    ==========
    gtfs_path: str
        The path (or URL) to a GTFS feed.
    cutoff: float
        The cutoff headway, above which a line won't be considered TOC.
    """
    df = bus_peak_frequencies(gtfs_path, **kwargs)
    # Find all the high frequency routes with frequency under a given cutoff.
    # TOC is 15 minutes, here we relax it a bit.
    high_frequency_routes = df[
        (df.am_peak_frequency <= cutoff) & (df.pm_peak_frequency <= cutoff)
    ]
    # Find all the routes that have high frequency in both directions.
    both_high_frequency = high_frequency_routes.groupby(level="route_id").size() == 2

    toc_routes = (
        high_frequency_routes.groupby(level="route_id")
        .agg(
            {
                "am_peak_frequency": "mean",
                "pm_peak_frequency": "mean",
                "geometry": "first",
                "route_short_name": "first",
                "agency": "first",
            }
        )
        .loc[both_high_frequency]
    )

    gdf = geopandas.GeoDataFrame(toc_routes, geometry="geometry")
    gdf.crs = {"init": f"epsg:{WGS84}"}
    return gdf


def bus_intersections(lines: geopandas.GeoDataFrame) -> geopandas.GeoDataFrame:
    """
    Calculate intersecting bus lines.
    This intersects using the route lines, but we should eventually
    use the bus stops, which would better handle buses that travel along
    the same road for a few blocks.

    Parameters
    ==========

    lines: geopandas.GeoDataFrame
        The geodataframe containing the routes in a "geometry" column.
    """
    # Perform a spatial join to find the lines taht inersect.
    intersecting_lines = (
        geopandas.sjoin(
            lines[["geometry"]], lines[["geometry"]], op="intersects", how="inner"
        )
        .reset_index()
        .rename(
            columns={
                "geometry": "geometry_a",
                "index": "route_a",
                "index_right": "route_b",
            }
        )
    )
    # Ignore the obvious lines that intersect with themselves.
    intersecting_lines = intersecting_lines[
        intersecting_lines.route_a != intersecting_lines.route_b
    ]

    # Restore agency, name, and geometry information for the matched lines.
    intersecting_lines = (
        intersecting_lines.merge(
            lines[["agency", "route_short_name"]],
            left_on="route_a",
            right_index=True,
            how="left",
        )
        .rename(columns={"agency": "agency_a", "route_short_name": "route_name_a"})
        .merge(
            lines[["agency", "route_short_name", "geometry"]],
            left_on="route_b",
            right_index=True,
            how="left",
        )
        .rename(
            columns={
                "agency": "agency_b",
                "route_short_name": "route_name_b",
                "geometry": "geometry_b",
            }
        )
    )

    # Calculate the geometry of the intersection, and drop the original
    # geometries.
    intersection = geopandas.GeoSeries(intersecting_lines.geometry_a).intersection(
        geopandas.GeoSeries(intersecting_lines.geometry_b)
    )
    intersecting_lines = intersecting_lines.set_geometry(intersection).drop(
        columns=["geometry_a", "geometry_b"]
    )

    # Drop duplicated intersections (e.g., line A intersects with B, line B intersects
    # with A)
    intersecting_lines = (
        intersecting_lines.assign(
            route_set=intersecting_lines.apply(
                lambda x: tuple({x.route_a, x.route_b}), axis=1
            ),
            geom_wkb=intersecting_lines.apply(lambda x: x.geometry.wkb, axis=1),
        )
        .drop_duplicates(subset=["route_set", "geom_wkb"])
        .drop(columns=["route_set", "geom_wkb"])
    )

    # Drop linestrings and multilinestrings, as lines traveling along
    # the same road are not considered an intersection.
    intersecting_lines = intersecting_lines[
        (intersecting_lines.geom_type != "LineString")
        & (intersecting_lines.geom_type != "MultiLineString")
    ].reset_index(drop=True)
    intersecting_lines = intersecting_lines.assign(
        geometry=geopandas.GeoSeries(
            intersecting_lines.apply(
                lambda x: shapely.geometry.GeometryCollection(
                    [g for g in x.geometry if g.type == "Point"]
                )
                if x.geometry.type == "GeometryCollection"
                else x.geometry,
                axis=1,
            )
        )
    )

    intersecting_lines.crs = lines.crs
    return intersecting_lines


def compute_toc_tiers_from_bus_intersections(
    intersections: geopandas.GeoDataFrame,
    clip: geopandas.GeoDataFrame,
    cushion: float = DEFAULT_CUSHION,
) -> geopandas.GeoDataFrame:
    """
    Given the bus intersections from bus_intersections, calculate the TOC
    tiers for each intersection. If the intersection is not eligible for a tier,
    an empty GeometryCollection is inserted.
    """

    # Project to feet
    intersections_feet = intersections.to_crs(epsg=SOCAL_FEET)

    # Given an intersection, compute all the tiers for it.
    def assign_tiers_to_bus_intersection(row):
        a_rapid = is_rapid_bus(row.agency_a, row.route_name_a)
        b_rapid = is_rapid_bus(row.agency_b, row.route_name_b)

        tier_4 = (
            shapely.geometry.GeometryCollection()
        )  # No bus-bus intersections have tier 4
        if a_rapid and b_rapid:
            tier_3 = row.geometry.buffer(1500 * cushion)
            tier_2 = row.geometry.buffer(2640 * cushion)
            tier_1 = shapely.geometry.GeometryCollection()
        elif a_rapid or b_rapid:
            tier_3 = row.geometry.buffer(750.0 * cushion)
            tier_2 = row.geometry.buffer(1500.0 * cushion)
            tier_1 = row.geometry.buffer(2640.0 * cushion)
        else:
            tier_3 = shapely.geometry.GeometryCollection()
            tier_2 = row.geometry.buffer(750.0 * cushion)
            tier_1 = row.geometry.buffer(2640.0 * cushion)
        return pandas.Series(
            {"tier_1": tier_1, "tier_2": tier_2, "tier_3": tier_3, "tier_4": tier_4}
        )

    intersection_tiers = pandas.concat(
        [
            intersections_feet,
            intersections_feet.apply(assign_tiers_to_bus_intersection, axis=1),
        ],
        axis=1,
    )

    # Reproject all of the columns back to 4326. This is somewhat awkward,
    # as geopandas doesn't handle multiple geometry column projections
    # gracefully.
    intersection_tiers = intersection_tiers.assign(
        tier_1=intersection_tiers.set_geometry("tier_1").to_crs(epsg=WGS84).tier_1,
        tier_2=intersection_tiers.set_geometry("tier_2").to_crs(epsg=WGS84).tier_2,
        tier_3=intersection_tiers.set_geometry("tier_3").to_crs(epsg=WGS84).tier_3,
        tier_4=intersection_tiers.set_geometry("tier_4").tier_4,
    ).to_crs(epsg=WGS84)

    intersection_tiers = intersection_tiers[
        intersection_tiers.set_geometry("tier_1").intersects(clip.iloc[0].geometry)
    ]
    return intersection_tiers


def compute_toc_tiers_from_metrolink_stations(
    stations: geopandas.GeoDataFrame,
    clip: geopandas.GeoDataFrame,
    cushion: float = DEFAULT_CUSHION,
) -> geopandas.GeoDataFrame:
    """
    Compute TOC Tiers from Metrolink stations, clipped to a boundary.

    Parameters
    ==========

    stations: geopandas.GeoDataFrame
        The Metrolink stations data frame.

    clip: geopandas.GeoDataFrame
        The boundary to clip the toc tiers to (probably the City of Los Angeles)
    """
    stations = stations.to_crs(epsg=SOCAL_FEET)
    stations = stations.assign(
        tier_4=[shapely.geometry.GeometryCollection()] * len(stations),
        tier_3=stations.geometry.buffer(750.0 * cushion),
        tier_2=stations.geometry.buffer(1500.0 * cushion),
        tier_1=stations.geometry.buffer(2640.0 * cushion),
    )
    stations = stations.assign(
        tier_1=stations.set_geometry("tier_1").to_crs(epsg=WGS84).tier_1,
        tier_2=stations.set_geometry("tier_2").to_crs(epsg=WGS84).tier_2,
        tier_3=stations.set_geometry("tier_3").to_crs(epsg=WGS84).tier_3,
        tier_4=stations.set_geometry("tier_4").tier_4,
    ).to_crs(epsg=WGS84)
    stations = stations[
        stations.set_geometry("tier_1").intersects(clip.iloc[0].geometry)
    ]

    return stations[
        [
            "Name",
            "ext_id",
            "description",
            "geometry",
            "tier_1",
            "tier_2",
            "tier_3",
            "tier_4",
        ]
    ].rename(columns={"ext_id": "station_id", "Name": "name"})


def compute_toc_tiers_from_metro_rail(
    stations: geopandas.GeoDataFrame,
    toc_buses: geopandas.GeoDataFrame,
    clip: geopandas.GeoDataFrame,
    cushion: float = DEFAULT_CUSHION,
) -> geopandas.GeoDataFrame:
    """
    Compute TOC tiers for metro rail stations.

    Parameters
    ==========

    stations: geopandas.GeoDataFrame
        The stations list for Metro Rail.
    toc_buses: geopandas.GeoDataFrame
        The list of bus lines that satisfies TOC.
    clip: geopandas.GeoDataFrame
        Clip the resulting geodataframe by this (probably the City of LA).
    """
    # Project into feet for the purpose of drawing buffers.
    stations_feet = stations.to_crs(epsg=SOCAL_FEET)

    # Find the stations that are the same, but for some reason given
    # different lines, put the intersecting line in a new column.
    station_intersections = geopandas.sjoin(
        stations_feet,
        stations_feet.set_geometry(stations_feet.buffer(660.0))[
            ["geometry", "LINE", "LINENUM"]
        ].rename(
            columns={
                "LINE": "intersecting_route_name",
                "LINENUM": "intersecting_route",
            }
        ),
        how="left",
        op="within",
    )

    # Also grab the intersections that are explicitly marked as such.
    station_intersections2 = (
        stations_feet[stations_feet.LINENUM2 != 0]
        .rename(columns={"LINENUM2": "intersecting_route"})
        .merge(
            stations_feet.set_index("LINENUM")[["LINE"]].rename(
                columns={"LINE": "intersecting_route_name"}
            ),
            how="left",
            left_on="intersecting_route",
            right_index=True,
        )
    )

    # Concatenate the two means of finding intersecting routes.
    station_intersections = pandas.concat(
        [station_intersections, station_intersections2], axis=0, sort=False
    )

    # Filter self intersections.
    station_intersections = (
        station_intersections[
            station_intersections.index_right != station_intersections.index
        ]
        .dropna(subset=["index_right"])
        .drop(columns=["index_right"])
        .drop_duplicates(subset=["TPIS_NAME"])
    )

    # Find all of the buses that are rapid buses, and determine
    # where their routes intersect with the stations.
    toc_rapid_buses = toc_buses[
        toc_buses.apply(lambda x: is_rapid_bus(x.agency, x.route_short_name), axis=1,)
    ]
    rapid_bus_intersections = (
        geopandas.sjoin(
            stations_feet.assign(buffered=stations_feet.buffer(660.0)).set_geometry(
                "buffered"
            ),
            toc_rapid_buses.to_crs(epsg=SOCAL_FEET)[
                ["geometry", "route_short_name", "agency"]
            ],
            how="left",
            op="intersects",
        )
        .rename(
            columns={
                "index_right": "intersecting_route",
                "route_short_name": "intersecting_route_name",
                "agency": "intersecting_route_agency",
            }
        )
        .set_geometry("geometry")
        .drop(columns=["buffered"])
    )

    # Concatenate the station intersections and the rapid bus intersections.
    stations = pandas.concat(
        [station_intersections, rapid_bus_intersections], axis=0, sort=False,
    ).rename(columns={"LINE": "line", "LINENUM": "line_id", "STATION": "station"})[
        [
            "line",
            "line_id",
            "station",
            "geometry",
            "intersecting_route",
            "intersecting_route_name",
            "intersecting_route_agency",
        ]
    ]

    # Determine tier 3 and tier 4 TOC zones.
    def assign_tiers_to_rail_stations(row):
        tier_2 = shapely.geometry.GeometryCollection()
        tier_1 = shapely.geometry.GeometryCollection()
        if not pandas.isna(row.intersecting_route):
            tier_4 = row.geometry.buffer(750.0 * cushion)
            tier_3 = row.geometry.buffer(2640.0 * cushion)
        else:
            tier_4 = shapely.geometry.GeometryCollection()
            tier_3 = row.geometry.buffer(2640.0 * cushion)
        return pandas.Series(
            {"tier_1": tier_1, "tier_2": tier_2, "tier_3": tier_3, "tier_4": tier_4}
        )

    station_toc_tiers = pandas.concat(
        [stations, stations.apply(assign_tiers_to_rail_stations, axis=1)], axis=1,
    )

    # Reproject back into WGS 84
    station_toc_tiers = station_toc_tiers.assign(
        tier_1=station_toc_tiers.set_geometry("tier_1").tier_1,
        tier_2=station_toc_tiers.set_geometry("tier_2").tier_2,
        tier_3=station_toc_tiers.set_geometry("tier_3").to_crs(epsg=WGS84).tier_3,
        tier_4=station_toc_tiers.set_geometry("tier_4").to_crs(epsg=WGS84).tier_4,
    ).to_crs(epsg=WGS84)

    # Drop all stations that don't intersect the City of LA and return.
    return station_toc_tiers[
        station_toc_tiers.set_geometry("tier_3").intersects(clip.iloc[0].geometry)
    ]


def join_with_toc_tiers(
    gdf: geopandas.GeoDataFrame,
    bus_toc_tiers: geopandas.GeoDataFrame,
    metrolink_toc_tiers: geopandas.GeoDataFrame,
    metro_rail_toc_tiers: geopandas.GeoDataFrame,
    tier: int,
) -> geopandas.GeoDataFrame:
    """
    Join a GeoDataFrame with TOC tiers, returning the table matched
    with TOC geometries.

    Parameters
    ==========
    gdf: geopandas.GeoDataFrame
        The geodataframe to join

    bus_toc_tiers: geopandas.GeoDataFrame
        The TOC tiers for bus lines. This should be similar to the results of
        compute_toc_tiers_from_bus_intersections.

    metrolink_toc_tiers: geopandas.GeoDataFrame
        The TOC tiers for Metrolink stations. This should be similar to the results
        of compute_toc_tiers_from_metrolink_stations.

    metro_rail_toc_tiers: geopandas.GeoDataFrame
        The TOC tiers for Metro rail stations. This should be similar to the
        results of compute_toc_tiers_from_metro_rail
    """
    assert tier >= 1 and tier <= 4
    current = gdf
    # trigger a spatial index build on the current df
    current.sindex
    colname = f"tier_{tier}"
    for other in [bus_toc_tiers, metrolink_toc_tiers, metro_rail_toc_tiers]:
        other = other.set_geometry(colname).drop(columns=["geometry"])
        if other[colname].is_empty.all():
            # This branch is a workaround for a geopandas bug joining on an
            # empty geometry column, cf. GH 1315
            current = geopandas.sjoin(current, other, how="left", op="contains").drop(
                columns=["index_right"]
            )
        else:
            current = geopandas.sjoin(current, other, how="left", op="within").drop(
                columns=["index_right"]
            )
    return current


# Utility function to determine whether a given line is a rapid bus.
def is_rapid_bus(agency, route_name):
    if agency == "Metro - Los Angeles":
        n = int(route_name.split("/")[0])
        return (n >= 700 and n < 800) or (n >= 900 and n < 1000)
    elif agency == "Culver CityBus":
        return route_name[-1] == "R"
    elif agency == "Big Blue Bus":
        return route_name[0] == "R"
    else:
        return False


if __name__ == "__main__":
    GTFS_URL = (
        "https://gitlab.com/LACMTA/gtfs_bus/-/raw/master/gtfs_bus.zip?inline=false"
    )
    gdf = bus_peak_frequencies(GTFS_URL)
    print(gdf.head())

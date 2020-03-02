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
    test_service = next(v for k, v in service_by_date.items() if k == test_date)
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
    gdf.crs = {"init": "epsg:4326"}
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
    gdf.crs = {"init": "epsg:4326"}
    return gdf


def bus_intersections(lines: geopandas.GeoDataFrame) -> geopandas.GeoDataFrame:
    """
    Calculate intersecting bus lines.

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
) -> geopandas.GeoDataFrame:
    """
    Given the bus intersections from bus_intersections, calculate the TOC
    tiers for each intersection. If the intersection is not eligible for a tier,
    an empty GeometryCollection is inserted.
    """

    # Project to feet
    intersections_feet = intersections.to_crs(epsg=2229)

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

    # Given an intersection, compute all the tiers for it.
    def assign_tiers_to_bus_intersection(row):
        a_rapid = is_rapid_bus(row.agency_a, row.route_name_a)
        b_rapid = is_rapid_bus(row.agency_b, row.route_name_b)

        tier_4 = None  # No bus-bus intersections have tier 4
        if a_rapid and b_rapid:
            tier_3 = row.geometry.buffer(1500)
            tier_2 = row.geometry.buffer(2640).difference(tier_3)
            tier_1 = shapely.geometry.GeometryCollection()
        elif a_rapid or b_rapid:
            tier_3 = row.geometry.buffer(750)
            tier_2 = row.geometry.buffer(1500).difference(tier_3)
            tier_1 = row.geometry.buffer(2640).difference(row.geometry.buffer(1500))
        else:
            tier_3 = shapely.geometry.GeometryCollection()
            tier_2 = row.geometry.buffer(750)
            tier_1 = row.geometry.buffer(2640)
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
        tier_1=intersection_tiers.set_geometry("tier_1").to_crs(epsg=4326).tier_1,
        tier_2=intersection_tiers.set_geometry("tier_2").to_crs(epsg=4326).tier_2,
        tier_3=intersection_tiers.set_geometry("tier_3").to_crs(epsg=4326).tier_3,
        tier_4=intersection_tiers.set_geometry("tier_4").tier_4,
    ).to_crs(epsg=4326)

    return intersection_tiers


def compute_toc_tiers_from_metrolink_stations(
    stations: geopandas.GeoDataFrame, clip: geopandas.GeoDataFrame
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
    stations = stations.to_crs(epsg=2229)
    stations = stations.assign(
        tier_4=geopandas.GeoSeries(),
        tier_3=stations.geometry.buffer(750.0),
        tier_2=stations.geometry.buffer(1500.0).difference(
            stations.geometry.buffer(750.0)
        ),
        tier_1=stations.geometry.buffer(2640.0).difference(
            stations.geometry.buffer(1500.0)
        ),
    )
    stations = stations.assign(
        tier_1=stations.set_geometry("tier_1").to_crs(epsg=4326).tier_1,
        tier_2=stations.set_geometry("tier_2").to_crs(epsg=4326).tier_2,
        tier_3=stations.set_geometry("tier_3").to_crs(epsg=4326).tier_3,
        tier_4=stations.set_geometry("tier_4").tier_4,
    ).to_crs(epsg=4326)
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


if __name__ == "__main__":
    GTFS_URL = (
        "https://gitlab.com/LACMTA/gtfs_bus/-/raw/master/gtfs_bus.zip?inline=false"
    )
    gdf = bus_peak_frequencies(GTFS_URL)
    print(gdf.head())

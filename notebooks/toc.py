"""
Utility functions for analyzing Transit Oriented Communities (TOC)
entitlements, following Measure JJJ.
"""

import fsspec
import geopandas
import pandas
import partridge

import datetime
import os
import typing

GTFS_FILE = os.path.join("/tmp", "gtfs.zip")
TEST_DATE = datetime.date(2020, 2, 18)


def line_peak_frequencies(
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

    return geopandas.GeoDataFrame(peak_frequency, geometry="geometry")


def toc_lines(gtfs_path: str, cutoff: float = 15.0, **kwargs) -> geopandas.GeoDataFrame:
    """
    Get the lines qualifying for TOC for a given GTFS path.

    Parameters
    ==========
    gtfs_path: str
        The path (or URL) to a GTFS feed.
    cutoff: float
        The cutoff headway, above which a line won't be considered TOC.
    """
    df = line_peak_frequencies(gtfs_path, **kwargs)
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

    return geopandas.GeoDataFrame(toc_routes, geometry="geometry")


if __name__ == "__main__":
    GTFS_URL = (
        "https://gitlab.com/LACMTA/gtfs_bus/-/raw/master/gtfs_bus.zip?inline=false"
    )
    gdf = line_peak_frequencies(GTFS_URL)
    print(gdf.head())

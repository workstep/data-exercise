import math
import os
import sys

from dotenv import load_dotenv

import googlemaps
import pandas as pd
from pony.orm import db_session

from logger import get_log
from db.models import db, LocationIndex, WageStat

load_dotenv()

BENCHMARK_SCALE = 60  # Miles
GMAPS_CLIENT_KEY = os.getenv("GMAPS_CLIENT_KEY")
LOG = get_log(__name__)


def get_data(path: str) -> pd.DataFrame:
    """Read data.
    Given a data path, return the dataframe.
    Args:
        path: data path
    Returns:
        The raw dataframe is returned.
    """
    try:
        raw_df = pd.read_csv(path)
        LOG.info(f"data: retrieved [{raw_df.shape[0]}] records")
    except Exception as error:
        LOG.exception(f"data: source data could not be loaded. {error}")
        sys.exit(1)

    # Check there's data to process
    if raw_df.shape[0] == 0:
        LOG.exception("data: source data empty.")
        sys.exit(1)

    return raw_df


def geocode_data(df: pd.DataFrame) -> pd.DataFrame:
    # GMAPS_CLIENT_KEY is stored in the environment by setup.py
    gmaps = googlemaps.Client(key=GMAPS_CLIENT_KEY)

    # Add columns in the dataframe for lat/lng
    df["lng"] = ""
    df["lat"] = ""

    for idx in range(len(df)):
        result = gmaps.geocode(df["address"][idx])
        try:
            df.at[idx, "lat"] = result[0]["geometry"]["location"]["lat"]
            df.at[idx, "lng"] = result[0]["geometry"]["location"]["lng"]
        except IndexError:
            # TODO: something better?
            LOG.debug("geocoding: address not found")

    return df


def index_locations(df: pd.DataFrame) -> pd.DataFrame:
    # Add a column in the dataframe for the LocationIndex foreign key
    df["location_index"] = ""

    for idx in range(len(df)):
        lat = df["lat"][idx]
        lng = df["lng"][idx]
        df.at[idx, "location_index"] = get_or_create_li_in_range(lat, lng, BENCHMARK_SCALE)

    return df


@db_session
def insert_wage_stats(df: pd.DataFrame):
    for idx in range(len(df)):
        WageStat(
            ctime=df["time_posted"][idx],
            location_index=df["location_index"][idx],
            wage=df["wage"][idx],
            title=df["title"][idx],
        )


@db_session
def get_or_create_li_in_range(lat, lng, distance_miles):
    from pony.orm import sql_debug
    sql_debug(True)

    miles_per_degree = 69  # approx for both lat and lng, at equator

    # Find the bounds of a representative polar coordinate grid "cell"
    lng_degrees_limit = distance_miles / (math.cos(math.radians(lat)) * miles_per_degree)
    lat_degrees_limit = distance_miles / miles_per_degree

    # Query any LocationIndex within those bounds
    index_ids = db.select(
        """
        id FROM location_index WHERE
            latitude > $lat - $lat_degrees_limit AND
            latitude < $lat + $lat_degrees_limit AND
            longitude > $lng - $lng_degrees_limit AND
            longitude < $lng + $lng_degrees_limit
        """
    )

    # If we found one, use it
    if index_ids:
        return index_ids[0]

    # Otherwise, create a new one for our coordinates
    li = LocationIndex(latitude=lat, longitude=lng)
    li.flush()

    return li.id

import sys, os

from dotenv import load_dotenv

import googlemaps
import pandas as pd
from pony.orm import db_session

from logger import get_log
from models import db, BenchmarkLocation

load_dotenv()

BENCHMARK_RADIUS = 60  # Miles
GMAPS_CLIENT_KEY = os.getenv('GMAPS_CLIENT_KEY')
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

    if raw_df.shape[0] == 0:
        LOG.exception(f"data: source data empty.")
        sys.exit(1)

    return raw_df


def parse_values(df: pd.DataFrame) -> pd.DataFrame:
    df['time_posted'] = pd.to_datetime(df['time_posted'])
    return df


def geocode_data(df: pd.DataFrame) -> pd.DataFrame:
    gmaps = googlemaps.Client(key=GMAPS_CLIENT_KEY)

    df['lng'] = ''
    df['lat'] = ''

    for idx in range(len(df)):
        result = gmaps.geocode(df['address'][idx])
        try:
            df.at[idx, 'lat'] = result[0]['geometry']['location']['lat']
            df.at[idx, 'lng'] = result[0]['geometry']['location']['lng']
        except IndexError:
            LOG.debug(f"geocoding: address not found")

    return df


def deduplicate_data(df: pd.DataFrame) -> pd.DataFrame:
    return df

def lookup_benchmarks(df: pd.DataFrame) -> pd.DataFrame:
    df['benchmark_location'] = ''

    for idx in range(len(df)):
        lat = df['lat'][idx]
        lng = df['lng'][idx]
        df.at[idx, 'benchmark_location'] = get_or_create_bl_in_range(lat, lng, BENCHMARK_RADIUS)

    print(df.head())

    return df

@db_session
def get_or_create_bl_in_range(lat, lng, distance_miles):
    from pony.orm import sql_debug
    sql_debug(True)
    bl_ids = db.select(
        """
        id FROM benchmark_location
        WHERE (
          3959 * acos(
            cos(radians($lat)) *
            cos(radians(latitude)) *
            cos(radians(longitude) - radians($lng))
            + sin(radians($lat)) * sin(radians(latitude))
          )
        ) <= $distance_miles
        """
    )
    if bl_ids:
        return bl_ids[0]

    bl = BenchmarkLocation(latitude=lat, longitude=lng)
    bl.flush()
    print(bl)
    return bl.id

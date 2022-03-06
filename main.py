import argparse

import pandas as pd

from vendor.pipeline import Pipeline
from utils import get_data, geocode_data, index_locations, insert_wage_stats
from logger import get_log

LOG = get_log(__name__)
pipeline = Pipeline()


@pipeline.task()
def get_raw_data(path: str) -> pd.DataFrame:
    return get_data(path)


@pipeline.task(depends_on=get_raw_data)
def parse_data(df: pd.DataFrame) -> pd.DataFrame:
    df["time_posted"] = pd.to_datetime(df["time_posted"])
    return df


@pipeline.task(depends_on=parse_data)
def geocode_addresses(df: pd.DataFrame) -> pd.DataFrame:
    """
    Utilizes Google Maps Geocoding API
    """
    geocoded_df = geocode_data(df)
    LOG.info(geocoded_df.head())

    return geocoded_df


@pipeline.task(depends_on=geocode_addresses)
def cleanse_data(df: pd.DataFrame) -> pd.DataFrame:
    # Don't consider wage events from our raw data which are
    # either too high or too low, since they're likely invalid
    return df[(df["wage"] > 8.0) & (df["wage"] < 150.0)]


@pipeline.task(depends_on=cleanse_data)
def index_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Indexing here will get or create LocationIndex as needed
    """
    return index_locations(df)


@pipeline.task(depends_on=index_data)
def store_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Utilizes db.db connector
    """
    insert_wage_stats(df)


def workflow():
    """
    Executes the pipeline workflow as configured above:

    get_raw_data -> parse_data -> geocode_addresses -> cleanse_data -> index_data -> store_data
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath")
    args = parser.parse_args()

    pipeline.run(f"{args.filepath}")


if __name__ == "__main__":
    workflow()

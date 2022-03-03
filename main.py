import argparse
import os

import pandas as pd

from vendor.pipeline import Pipeline
from logger import get_log
from utils import get_data, geocode_data, index_locations, insert_wage_stats


pipeline = Pipeline()

@pipeline.task()
def get_raw_data(path: str) -> pd.DataFrame:
    return get_data(path)

@pipeline.task(depends_on=get_raw_data)
def parse_data(df: pd.DataFrame) -> pd.DataFrame:
    df['time_posted'] = pd.to_datetime(df['time_posted'])
    return df

@pipeline.task(depends_on=parse_data)
def geocode_addresses(df: pd.DataFrame) -> pd.DataFrame:
    geocoded_df = geocode_data(df)
    print(geocoded_df.head())
    return geocoded_df

@pipeline.task(depends_on=geocode_addresses)
def cleanse_data(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df['wage'] > 8.0) & (df['wage'] < 50.0)]

@pipeline.task(depends_on=cleanse_data)
def index_data(df: pd.DataFrame) -> pd.DataFrame:
    return index_locations(df)

@pipeline.task(depends_on=index_data)
def store_data(df: pd.DataFrame) -> pd.DataFrame:
    insert_wage_stats(df)

def workflow():
    parser = argparse.ArgumentParser()
    parser.add_argument('filepath')
    args = parser.parse_args()

    pipeline.run(f'{args.filepath}')


if __name__ == '__main__':
    workflow()

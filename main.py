import os

import pandas as pd

from pipeline import Pipeline
from logger import get_log
from utils import get_data, geocode_data, parse_values, deduplicate_data, lookup_benchmarks


pipeline = Pipeline()

@pipeline.task()
def get_raw_data(path: str) -> pd.DataFrame:
    raw_df = get_data(path)
    return raw_df

@pipeline.task(depends_on=get_raw_data)
def parse_data(path: str) -> pd.DataFrame:
    parsed_df = parse_values(path)
    return parsed_df

@pipeline.task(depends_on=parse_data)
def geocode_addresses(raw_df: pd.DataFrame) -> pd.DataFrame:
    geocoded_df = geocode_data(raw_df)
    print(geocoded_df.head())
    return geocoded_df

@pipeline.task(depends_on=geocode_addresses)
def cleanse_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    cleansed_df = deduplicate_data(raw_df)
    return cleansed_df

@pipeline.task(depends_on=cleanse_data)
def benchmark_locations(raw_df: pd.DataFrame) -> pd.DataFrame:
    return lookup_benchmarks(raw_df)


#@pipeline.task(depends_on=get_metric)
#def save_metric(metric_df: pd.DataFrame) -> None:
#    metric_df.to_csv('/demo/data/output.csv')


def workflow():
    pipeline.run('data.csv')

if __name__ == '__main__':
    workflow()

import datetime as dt
import os

import numpy as np

from sentinelhub import read_data
from sentinelhub.data_utils import get_failed_statistical_requests, statistical_to_dataframe

column_types = {
    "ndvi_B0_min": float,
    "ndvi_B0_max": float,
    "ndvi_B0_mean": float,
    "ndvi_B0_stDev": float,
    "ndvi_B0_sampleCount": np.int64,
    "ndvi_B0_noDataCount": np.int64,
    "ndvi_B0_percentiles_50.0": float,
    "ndvi_B0_bins": list,
    "ndvi_B0_counts": list,
    "interval_from": dt.date,
    "interval_to": dt.date,
    "identifier": str,
}


def test_statistical_to_dataframe() -> None:
    batch_stat_results = read_data(os.path.join(os.path.dirname(__file__), "TestInputs", "batch_stat_results.json"))
    df = statistical_to_dataframe(batch_stat_results)
    num_polys = len(set(df["identifier"]))
    num_columns = len(df.columns)
    num_valid_rows = len(df)
    assert num_polys == 2
    assert num_columns == 12
    assert num_valid_rows == 2
    for column, data_type in column_types.items():
        assert isinstance(df[column].iloc[0], data_type)


def test_get_failed_statistical_requests() -> None:
    batch_stat_failed_results = read_data(
        os.path.join(os.path.dirname(__file__), "TestInputs", "batch_stat_failed_results.json")
    )
    failed_requests = get_failed_statistical_requests(batch_stat_failed_results)
    assert len(failed_requests) == 1

import datetime as dt
import os

import numpy as np
import pytest

from sentinelhub import read_data
from sentinelhub.data_utils import get_failed_statistical_requests, statistical_to_dataframe

column_type_pairs = [
    (float, ["ndvi_B0_min", "ndvi_B0_max", "ndvi_B0_mean", "ndvi_B0_stDev", "ndvi_B0_percentiles_50.0"]),
    (np.int64, ["ndvi_B0_sampleCount", "ndvi_B0_noDataCount"]),
    (list, ["ndvi_B0_bins", "ndvi_B0_counts"]),
    (dt.date, ["interval_from", "interval_to"]),
    (str, ["identifier"]),
]


def test_statistical_to_dataframe(input_folder: str) -> None:
    batch_stat_results_path = os.path.join(input_folder, "batch_stat_results.json")
    batch_stat_results = read_data(batch_stat_results_path)
    df = statistical_to_dataframe(batch_stat_results)
    assert len(set(df["identifier"])) == 2, "Wrong number of polygons"
    assert len(df.columns) == 12, "Wrong number of columns"
    assert len(df) == 2, "Wrong number of valid rows"
    for data_type, columns in column_type_pairs:
        assert all(isinstance(df[column].iloc[0], data_type) for column in columns), "Wrong data type of columns"


@pytest.mark.parametrize(
    "result_file, expected_length",
    [
        ("batch_stat_failed_results.json", 3),
        ("batch_stat_results.json", 0),
        ("normal_stat_partial_result.json", 1),
        ("normal_stat_failed_result.json", 1),
        ("normal_stat_result.json", 0),
    ],
)
def test_get_failed_statistical_requests(input_folder: str, result_file: str, expected_length: int) -> None:
    stat_failed_results_path = os.path.join(input_folder, result_file)
    stat_failed_results = read_data(stat_failed_results_path)
    failed_requests = get_failed_statistical_requests(stat_failed_results)
    assert len(failed_requests) == expected_length, "Wrong number of failed requests"

"""
Utility tools for writing unit tests for packages which rely on `sentinelhub-py`
"""
import os
from typing import Optional, Tuple

import numpy as np
from pytest import approx


def get_input_folder(current_file: str) -> str:
    """Use fixtures if possible. This is meant only for test cases"""
    return os.path.join(os.path.dirname(os.path.realpath(current_file)), "TestInputs")


def get_output_folder(current_file: str) -> str:
    """Use fixtures if possible. This is meant only for test cases"""
    return os.path.join(os.path.dirname(os.path.realpath(current_file)), "TestOutputs")


def test_numpy_data(
    data: Optional[np.ndarray] = None,
    exp_shape: Optional[Tuple[int, ...]] = None,
    exp_dtype: Optional[np.dtype] = None,
    exp_min: Optional[float] = None,
    exp_max: Optional[float] = None,
    exp_mean: Optional[float] = None,
    exp_median: Optional[float] = None,
    exp_std: Optional[float] = None,
    delta: Optional[float] = None,
) -> None:
    """Validates basic statistics of data array

    :param data: Data array
    :param exp_shape: Expected shape
    :param exp_dtype: Expected dtype
    :param exp_min: Expected minimal value
    :param exp_max: Expected maximal value
    :param exp_mean: Expected mean value
    :param exp_median: Expected median value
    :param exp_std: Expected standard deviation value
    :param delta: Precision of validation (relative). If not set, it will be set automatically
    """
    if data is None:
        return
    delta = delta if delta is not None else 1e-4

    stats_suite = {
        "shape": (lambda array: array.shape, exp_shape),
        "dtype": (lambda array: array.dtype, exp_dtype),
        "min": (np.nanmin, exp_min),
        "max": (np.nanmax, exp_max),
        "mean": (np.nanmean, exp_mean),
        "median": (np.nanmedian, exp_median),
        "std": (np.nanstd, exp_std),
    }

    is_precise = {"shape", "dtype"}

    data_stats, exp_stats = {}, {}
    for name, (func, expected) in stats_suite.items():
        if expected is not None:
            data_stats[name] = func(data)  # type: ignore # unknown function
            exp_stats[name] = expected if name in is_precise else approx(expected, rel=delta)

    assert data_stats == exp_stats, "Statistics differ from expected values"

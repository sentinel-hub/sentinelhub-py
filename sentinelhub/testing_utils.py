"""
Utility tools for writing unit tests for packages which rely on `sentinelhub-py`
"""
import os
from typing import Any, Callable, Dict, Optional, Tuple, Union

import numpy as np
from pytest import approx

from .exceptions import deprecated_function


def get_input_folder(current_file: str) -> str:
    """Use fixtures if possible. This is meant only for test cases"""
    return os.path.join(os.path.dirname(os.path.realpath(current_file)), "TestInputs")


def get_output_folder(current_file: str) -> str:
    """Use fixtures if possible. This is meant only for test cases"""
    return os.path.join(os.path.dirname(os.path.realpath(current_file)), "TestOutputs")


@deprecated_function(message_suffix="Use `assert_statistics_match` instead.")
def test_numpy_data(
    data: Optional[np.ndarray] = None,
    exp_shape: Optional[Tuple[int, ...]] = None,
    exp_dtype: Union[None, type, np.dtype] = None,
    exp_min: Optional[float] = None,
    exp_max: Optional[float] = None,
    exp_mean: Optional[float] = None,
    exp_median: Optional[float] = None,
    exp_std: Optional[float] = None,
    delta: float = 1e-4,
) -> None:
    """Deprecated version of `assert_statistics_match`."""
    if data is None:
        return
    assert_statistics_match(
        data, exp_shape, exp_dtype, exp_min, exp_max, exp_mean, exp_median, exp_std, rel_delta=delta
    )


def assert_statistics_match(
    data: np.ndarray,
    exp_shape: Optional[Tuple[int, ...]] = None,
    exp_dtype: Union[None, type, np.dtype] = None,
    exp_min: Optional[float] = None,
    exp_max: Optional[float] = None,
    exp_mean: Optional[float] = None,
    exp_median: Optional[float] = None,
    exp_std: Optional[float] = None,
    rel_delta: Optional[float] = None,
    abs_delta: Optional[float] = None,
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
    :param rel_delta: Precision of validation (relative)
    :param abs_delta: Precision of validation (absolute)
    """

    stats_suite: Dict[str, Tuple[Callable[[np.ndarray], Any], Any]] = {
        "shape": (lambda array: array.shape, exp_shape),
        "dtype": (lambda array: array.dtype, exp_dtype),
        "min": (np.nanmin, exp_min),
        "max": (np.nanmax, exp_max),
        "mean": (np.nanmean, exp_mean),
        "median": (np.nanmedian, exp_median),
        "std": (np.nanstd, exp_std),
    }

    is_precise = {"shape", "dtype"}

    data_stats: Dict[str, Any] = {}
    exp_stats: Dict[str, Any] = {}
    for name, (func, expected) in stats_suite.items():
        if expected is not None:
            data_stats[name] = func(data)
            exp_stats[name] = expected if name in is_precise else approx(expected, rel=rel_delta, abs=abs_delta)

    assert data_stats == exp_stats, "Statistics differ from expected values"

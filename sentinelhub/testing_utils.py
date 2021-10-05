"""
Utility tools for writing unit tests for packages which rely on `sentinelhub-py`
"""
import os

import numpy as np
from pytest import approx


def get_input_folder(current_file):
    """ Use fixtures if possible. This is meant only for test cases
    """
    return os.path.join(os.path.dirname(os.path.realpath(current_file)), 'TestInputs')


def get_output_folder(current_file):
    """ Use fixtures if possible. This is meant only for test cases
    """
    return os.path.join(os.path.dirname(os.path.realpath(current_file)), 'TestOutputs')


def test_numpy_data(data=None, exp_shape=None, exp_dtype=None, exp_min=None, exp_max=None, exp_mean=None,
                    exp_median=None, exp_std=None, delta=None):
    """ Validates basic statistics of data array

    :param data: Data array
    :type data: numpy.ndarray
    :param exp_shape: Expected shape
    :type exp_shape: tuple(int)
    :param exp_dtype: Expected dtype
    :type exp_dtype: numpy.dtype
    :param exp_min: Expected minimal value
    :type exp_min: float
    :param exp_max: Expected maximal value
    :type exp_max: float
    :param exp_mean: Expected mean value
    :type exp_mean: float
    :param exp_median: Expected median value
    :type exp_median: float
    :param exp_std: Expected standard deviation value
    :type exp_std: float
    :param delta: Precision of validation (relative). If not set, it will be set automatically
    :type delta: float
    """
    if data is None:
        return
    delta = delta if delta is not None else 1e-4

    stats_suite = {
        'shape': (lambda array: array.shape, exp_shape),
        'dtype': (lambda array: array.dtype, exp_dtype),
        'min': (np.nanmin, exp_min),
        'max': (np.nanmax, exp_max),
        'mean': (np.nanmean, exp_mean),
        'median': (np.nanmedian, exp_median),
        'std': (np.nanstd, exp_std),
    }

    is_precise = {'shape', 'dtype'}

    data_stats, exp_stats = {}, {}
    for name, (func, expected) in stats_suite.items():
        if expected is not None:
            data_stats[name] = func(data)
            exp_stats[name] = expected if name in is_precise else approx(expected, rel=delta)

    assert data_stats == exp_stats, 'Statistics differ from expected values'

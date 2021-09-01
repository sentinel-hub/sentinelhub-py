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
                    exp_median=None, delta=None):
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
    :param delta: Precision of validation. If not set, it will be set automatically
    :type delta: float
    """
    if data is None:
        return
    if delta is None:
        delta = 1e-1 if np.issubdtype(data.dtype, np.integer) else 1e-4

    for exp_stat, stat_val, stat_name in [(exp_shape, data.shape, 'shape'), (exp_dtype, data.dtype, 'dtype')]:
        if exp_stat is not None:
            assert stat_val == exp_stat, f'Expected {stat_name} {exp_stat}, got {stat_val}'

    data = data[~np.isnan(data)]
    for exp_stat, stat_func, stat_name in [
        (exp_min, np.amin, 'min'), (exp_max, np.amax, 'max'),
        (exp_mean, np.mean, 'mean'), (exp_median, np.median, 'median')
    ]:
        if exp_stat is not None:
            stat_val = stat_func(data)
            assert stat_val == approx(exp_stat, abs=delta), f'Expected {stat_name} {exp_stat}, got {stat_val}'

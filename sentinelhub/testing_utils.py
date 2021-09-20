"""
Utility tools for writing unit tests for packages which rely on `sentinelhub-py`
"""
import os

import numpy as np
from numpy.lib.nanfunctions import nanvar
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
                    exp_median=None, exp_std=-1, delta=None):
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
    :param delta: Precision of validation. If not set, it will be set automatically
    :type delta: float
    """
    if data is None:
        return
    if delta is None:
        delta = 1e-1 if np.issubdtype(data.dtype, np.integer) else 1e-4

    stats_suite = {
        'shape': (lambda data: data.shape, exp_shape),
        'dtype': (lambda data: data.dtype, exp_dtype),
        'min': (np.nanmin, exp_min),
        'max': (np.nanmax, exp_max),
        'mean': (np.nanmean, exp_mean),
        'median': (np.nanmedian, exp_median),
        'std': (np.nanstd, exp_std),
    }

    data_stats, exp_stats = {}, {}
    for name, (func, expected) in stats_suite.items():
        if expected is not None:
            data_stats[name] = func(data)
            exp_stats[name] = expected

    for name in data_stats:
        assert data_stats[name] == approx(exp_stats[name], abs=delta), \
            f'Statistic `{name}` does not approximately match when comparing {data_stats} with expectations {exp_stats}'

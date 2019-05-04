"""
Utility tools for writing unit tests for packages which rely on `sentinelhub-py`
"""

import unittest
import os
import shutil
import logging
import inspect

import numpy as np

from .config import SHConfig


class TestSentinelHub(unittest.TestCase):
    """ Class implementing common functionalities of unit tests for working with `sentinelhub-py` package:

    - reading configuration parameters from environmental variables and saving them to `config.json`,
    - setting logger,
    - handling input and output data folders,
    - method for testing statistics of a numpy data array.
    """

    CONFIG = None
    LOGGER = None
    INPUT_FOLDER = None
    OUTPUT_FOLDER = None

    INPUT_FOLDER = None
    OUTPUT_FOLDER = None
    CLEAR_OUTPUTS = True

    @classmethod
    def setUpClass(cls):
        """ A general set up class

        Use ``super().setUpClass()`` in every class which inherits ``TestSentinelHub``
        """
        if cls.__name__ == TestSentinelHub.__name__:
            return

        cls.INPUT_FOLDER = os.path.join(os.path.dirname(inspect.getsourcefile(cls)), 'TestInputs')
        cls.OUTPUT_FOLDER = os.path.join(os.path.dirname(inspect.getsourcefile(cls)), 'TestOutputs')

        if cls.CONFIG is None:
            cls.CONFIG = cls._config_with_environment_variables()

        if cls.LOGGER is None:
            logging.basicConfig(level=logging.INFO,
                                format='%(asctime)-15s %(module)s:%(lineno)d [%(levelname)s] %(funcName)s  %(message)s')
            cls.LOGGER = logging.getLogger(__name__)

    @staticmethod
    def _config_with_environment_variables():
        """ Reads configuration parameters from environmental variables
        """
        config = SHConfig()
        for param in config.get_params():
            env_variable = param.upper()
            if os.environ.get(env_variable):
                setattr(config, param, os.environ.get(env_variable))
        config.save()
        return config

    @classmethod
    def tearDownClass(cls):
        if cls.CLEAR_OUTPUTS and cls.OUTPUT_FOLDER:
            shutil.rmtree(cls.OUTPUT_FOLDER, ignore_errors=True)

    def test_numpy_data(self, data=None, exp_shape=None, exp_dtype=None, exp_min=None, exp_max=None, exp_mean=None,
                        exp_median=None, delta=None, test_name=''):
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
        :param test_name: Name of the test case
        :type test_name: str
        """
        if data is None:
            return
        if delta is None:
            delta = 1e-1 if np.issubdtype(data.dtype, np.integer) else 1e-4

        for exp_stat, stat_val, stat_name in [(exp_shape, data.shape, 'shape'), (exp_dtype, data.dtype, 'dtype')]:
            if exp_stat is not None:
                with self.subTest(msg='Test case {}'.format(test_name)):
                    self.assertEqual(stat_val, exp_stat,
                                     msg='Expected {} {}, got {}'.format(stat_name, exp_stat, stat_val))

        for exp_stat, stat_func, stat_name in [(exp_min, np.amin, 'min'), (exp_max, np.amax, 'max'),
                                               (exp_mean, np.mean, 'mean'), (exp_median, np.median, 'median')]:
            if exp_stat is not None:
                stat_val = stat_func(data)
                with self.subTest(msg='Test case {}'.format(test_name)):
                    self.assertAlmostEqual(stat_val, exp_stat, delta=delta,
                                           msg='Expected {} {}, got {}'.format(stat_name, exp_stat, stat_val))


class TestCaseContainer:
    """ Class for storing expected statistics for a single test case

    :param name: Name of a test case
    :type name: str
    :param request: A class which provides the data for testing
    :type request: object
    :param stats: Any other parameters
    """
    def __init__(self, name, request, **stats):
        self.name = name
        self.request = request

        for stat_name, stat_value in stats.items():
            setattr(self, stat_name, stat_value)

    def __getattr__(self, key):
        """ Fallback if the attribute is missing - in that case `None` is returned
        """

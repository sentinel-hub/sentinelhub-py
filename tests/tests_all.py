import unittest
import os
import shutil
import logging

import numpy as np

from sentinelhub import SHConfig


def _save_environment_variables():
    config = SHConfig()
    for attr in ['INSTANCE_ID', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']:
        if os.environ.get(attr):
            setattr(config, attr.lower(), os.environ.get(attr))
    config.save()


class TestSentinelHub(unittest.TestCase):

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)-15s %(module)s:%(lineno)d [%(levelname)s] %(funcName)s  %(message)s')
    LOGGER = logging.getLogger(__name__)

    _save_environment_variables()

    INPUT_FOLDER = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'TestInputs')
    OUTPUT_FOLDER = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'TestOutputs')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.OUTPUT_FOLDER, ignore_errors=True)

    def test_numpy_stats(self, data=None, exp_min=None, exp_max=None, exp_mean=None, exp_median=None, test_name=''):
        """ Validates data over basic 4 statistics
        """
        if data is None:
            return
        delta = 1e-1 if np.issubdtype(data.dtype, np.integer) else 1e-4

        for exp_stat, stat_func, stat_name in [(exp_min, np.amin, 'min'), (exp_max, np.amax, 'max'),
                                               (exp_mean, np.mean, 'mean'), (exp_median, np.median, 'median')]:
            if exp_stat is not None:
                stat_val = stat_func(data)
                with self.subTest(msg='Test case {}'.format(test_name)):
                    self.assertAlmostEqual(stat_val, exp_stat, delta=delta,
                                           msg='Expected {} {}, got {}'.format(stat_name, exp_stat, stat_val))


if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = loader.discover(os.path.dirname(os.path.realpath(__file__)))
    runner = unittest.TextTestRunner()
    runner.run(suite)

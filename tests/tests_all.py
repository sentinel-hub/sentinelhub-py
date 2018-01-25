import unittest
import os
import shutil
import logging


class TestSentinelHub(unittest.TestCase):

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)-15s %(module)s:%(lineno)d [%(levelname)s] %(funcName)s  %(message)s')
    LOGGER = logging.getLogger(__name__)

    INSTANCE_ID = os.environ.get('INSTANCE_ID')
    INPUT_FOLDER = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'TestInputs')
    OUTPUT_FOLDER = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'TestOutputs')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.OUTPUT_FOLDER, ignore_errors=True)


if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = loader.discover(os.path.dirname(os.path.realpath(__file__)))
    runner = unittest.TextTestRunner()
    runner.run(suite)

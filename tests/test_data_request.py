import logging
import unittest
import os
import errno

from sentinelhub.data_request import WcsRequest
from sentinelhub.common import BBox
from sentinelhub.constants import CRS


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)-15s %(module)s:%(lineno)d [%(levelname)s] %(funcName)s  %(message)s')
LOGGER = logging.getLogger(__name__)

INSTANCE_ID = os.environ.get('INSTANCE_ID')


class TestDataRequest(unittest.TestCase):
    def test_init(self):

        valid_dir_name = 'test_dir'
        try:
            os.makedirs(os.path.abspath(valid_dir_name))
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                self.fail('Failed to create test directory at {}'.format(os.path.abspath(valid_dir_name)))

        bbox = BBox((8.655, 111.7, 8.688, 111.6), crs=CRS.WGS84)
        data_request = WcsRequest(data_folder=valid_dir_name, bbox=bbox,
                                  layer='ALL_BANDS', instance_id=INSTANCE_ID)

        self.assertEqual(valid_dir_name, data_request.data_folder,
                         msg="Expected {}, got {}".format(valid_dir_name, data_request.data_folder))

        os.rmdir(os.path.abspath(valid_dir_name))

        data_request = WcsRequest(data_folder=valid_dir_name, bbox=bbox,
                                  layer='ALL_BANDS', instance_id=INSTANCE_ID)

        self.assertEqual(True, os.path.exists(os.path.abspath(data_request.data_folder)),
                         msg="Expected output dir {} to exist.".format(data_request.data_folder))

        os.rmdir(os.path.abspath(valid_dir_name))

        invalid_dir_name = '/test_dir'
        try:
            os.makedirs(os.path.abspath(invalid_dir_name))
        except OSError:
            try:
                WcsRequest(data_folder=invalid_dir_name,
                                           bbox=bbox,
                                           layer='ALL_BANDS')
            except ValueError as err:
                LOGGER.error(err)


if __name__ == '__main__':
    unittest.main()

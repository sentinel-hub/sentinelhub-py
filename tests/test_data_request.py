import unittest
from tests_all import TestSentinelHub

from sentinelhub.data_request import WcsRequest
from sentinelhub.common import BBox
from sentinelhub.constants import CRS


class TestDataRequest(TestSentinelHub):
    def test_init(self):

        bbox = BBox((8.655, 111.7, 8.688, 111.6), crs=CRS.WGS84)
        data_request = WcsRequest(data_folder=self.OUTPUT_FOLDER, bbox=bbox,
                                  layer='ALL_BANDS', instance_id=self.INSTANCE_ID)

        self.assertEqual(self.OUTPUT_FOLDER, data_request.data_folder,
                         msg="Expected {}, got {}".format(self.OUTPUT_FOLDER, data_request.data_folder))
        self.assertTrue(isinstance(data_request.get_filename_list(), list), "Expected a list")
        self.assertTrue(isinstance(data_request.get_url_list(), list), "Expected a list")
        self.assertTrue(data_request.is_valid_request(), "Request should be valid")


if __name__ == '__main__':
    unittest.main()

import unittest
import os.path
from tests_all import TestSentinelHub

from sentinelhub import WcsRequest, AwsProductRequest, BBox, CRS, get_file_list, get_folder_list


class TestDataRequest(TestSentinelHub):
    def test_init(self):

        bbox = BBox((8.655, 111.7, 8.688, 111.6), crs=CRS.WGS84)
        data_request = WcsRequest(data_folder=self.OUTPUT_FOLDER, bbox=bbox, layer='BANDS-S2-L1C')
        data_request.create_request()  # This method is used by s2cloudless, don't rename it

        self.assertEqual(self.OUTPUT_FOLDER, data_request.data_folder,
                         msg="Expected {}, got {}".format(self.OUTPUT_FOLDER, data_request.data_folder))
        self.assertTrue(isinstance(data_request.get_filename_list(), list), "Expected a list")
        self.assertTrue(isinstance(data_request.get_url_list(), list), "Expected a list")
        self.assertTrue(data_request.is_valid_request(), "Request should be valid")


class TestDataRequestSaving(TestSentinelHub):

    @classmethod
    def setUpClass(cls):
        cls.product_id = 'S2A_MSIL1C_20180113T001101_N0206_R073_T55KGP_20180113T013328.SAFE'
        metafiles = 'inspire '
        cls.request_without_folder = AwsProductRequest(bands='', metafiles=metafiles, safe_format=True,
                                                       product_id=cls.product_id)
        cls.request_with_folder = AwsProductRequest(data_folder=cls.OUTPUT_FOLDER, bands='', metafiles=metafiles,
                                                    safe_format=True, product_id=cls.product_id)

    def test_saving_responses(self):
        try:
            data = self.request_without_folder.get_data()
            self.assertTrue(isinstance(data, list), "Expected a list")
            self.assertEqual(len(data), 1, "Expected a list of length 1")
        except ValueError:
            self.fail("get_data method with save_data=False should work without specifying data_folder")
        self.assertRaises(ValueError, lambda: self.request_without_folder.get_data(save_data=True, redownload=True))
        self.assertRaises(ValueError, self.request_without_folder.save_data)

        try:
            data = self.request_with_folder.get_data(save_data=True)
            self.assertTrue(isinstance(data, list), "Expected a list")
            self.assertEqual(len(data), 1, "Expected a list of length 1")
            product_folder = os.path.join(self.OUTPUT_FOLDER, self.product_id)
            self.assertEqual(len(get_folder_list(product_folder)), 5, "Expected to create 5 folders")
            self.assertEqual(len(get_file_list(product_folder)), 1, "Expected to create 1 file")
        except ValueError:
            self.fail("Expected to obtain and save data")


if __name__ == '__main__':
    unittest.main()

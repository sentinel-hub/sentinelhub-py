import logging
import unittest
import shutil

from sentinelhub.data_request import AwsTileRequest, AwsProductRequest
from sentinelhub.aws_safe import SafeTile

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)-15s %(module)s:%(lineno)d [%(levelname)s] %(funcName)s  %(message)s')


class TestAwsSafeTile(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.request = AwsTileRequest(data_folder='TestOutputs', tile='10UEV',
                                     metafiles='metadata,tileInfo', time='2016-01-09', safe_format=True)
        cls.request.save_data(redownload=True)
        cls.filename_list = cls.request.get_filename_list()

    def test_return_type(self):
        self.assertTrue(isinstance(self.filename_list, list), "Expected a list")
        self.assertEqual(len(self.filename_list), 14, "Expected a list of length 14")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('./TestOutputs/', ignore_errors=True)


class TestAwsSafeProduct(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.request = AwsProductRequest(data_folder='TestOutputs', bands='B01', safe_format=True,
                                        product_id='S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_'
                                                   '20160103T171947')
        cls.data = cls.request.get_data(save_data=True, redownload=True)

    def test_return_type(self):
        self.assertTrue(isinstance(self.data, list), "Expected a list")
        self.assertEqual(len(self.data), 125, "Expected a list of length 125")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('./TestOutputs/', ignore_errors=True)


class TestPartialAwsSafeProduct(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        bands = 'B12'
        metafiles = 'manifest,preview/B02'
        tile = '1WCV'
        cls.request = AwsProductRequest(data_folder='TestOutputs', bands=bands,
                                        metafiles=metafiles, safe_format=True, tile_list=[tile],
                                        product_id='S2A_MSIL1C_20171010T003621_N0205_R002_T01WCV_20171010T003615')
        cls.data = cls.request.get_data(save_data=True, redownload=True)

    def test_return_type(self):
        self.assertTrue(isinstance(self.data, list), "Expected a list")
        self.assertEqual(len(self.data), 2, "Expected a list of length 2")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('./TestOutputs/', ignore_errors=True)


if __name__ == '__main__':
    unittest.main()

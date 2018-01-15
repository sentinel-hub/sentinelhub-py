import logging
import unittest
import shutil

from sentinelhub.data_request import AwsTileRequest, AwsProductRequest
from sentinelhub.aws import AwsTile

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)-15s %(module)s:%(lineno)d [%(levelname)s] %(funcName)s  %(message)s')


class TestAwsTile(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.request = AwsTileRequest(data_folder='TestOutputs', bands='B01',
                                     metafiles='metadata,tileInfo', tile='10UEV', time='2016-01-09', aws_index=0)
        cls.data = cls.request.get_data(save_data=True, redownload=True)

    def test_return_type(self):
        self.assertTrue(isinstance(self.data, list), "Expected a list")
        self.assertEqual(len(self.data), 3, "Expected a list of length 3")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('./TestOutputs/', ignore_errors=True)


class TestAwsProduct(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.request = AwsProductRequest(data_folder='TestOutputs', bands='B10',
                                        metafiles='metadata,tileInfo,productInfo',
                                        product_id='S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_'
                                                   '20160103T171947')
        cls.data = cls.request.get_data(save_data=True, redownload=True)

    def test_return_type(self):
        self.assertTrue(isinstance(self.data, list), "Expected a list")
        self.assertEqual(len(self.data), 50, "Expected a list of length 50")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('./TestOutputs/', ignore_errors=True)


class TestPartialAwsProduct(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        bands = 'B12'
        metafiles = 'manifest,preview/B02'
        tile = '1WCV'
        cls.request = AwsProductRequest(data_folder='TestOutputs', bands=bands,
                                        metafiles=metafiles, tile_list=[tile],
                                        product_id='S2A_MSIL1C_20171010T003621_N0205_R002_T01WCV_20171010T003615')
        cls.data = cls.request.get_data(save_data=True, redownload=True)

    def test_return_type(self):
        self.assertTrue(isinstance(self.data, list), "Expected a list")
        self.assertEqual(len(self.data), 3, "Expected a list of length 3")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('./TestOutputs/', ignore_errors=True)


if __name__ == '__main__':
    unittest.main()

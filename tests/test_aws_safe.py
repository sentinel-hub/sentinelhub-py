import unittest
from tests_all import TestSentinelHub

import os.path
from sentinelhub.io_utils import read_data, write_data
from sentinelhub.data_request import AwsTileRequest, AwsProductRequest


class TestSafeFormat(TestSentinelHub):

    class SafeTestCase:
        """
        Container for each test case of sentinelhub OGC functionalities
        """
        def __init__(self, name, request):
            self.name = name
            self.request = request

        def get_filename(self):
            return os.path.join(TestSentinelHub.INPUT_FOLDER, '{}.csv'.format(self.name))

        def get_request_data(self):
            return [(req.url, req.filename[:]) for req in self.request.get_download_list()]

        def load_truth(self):
            return [tuple(item) for item in read_data(self.get_filename())]

        def save_truth(self):
            """ Use this method only to create new unittests
            """
            write_data(self.get_filename(), self.get_request_data())

        @staticmethod
        def compare_safe_struct(true_safe, req_safe):
            """ This method compares two lists with safe structure
            """
            true_set = set(true_safe)
            req_set = set(req_safe)
            missing = [item for item in true_set if item not in req_set]
            redundant = [item for item in req_set if item not in true_set]
            if len(missing) + len(redundant) > 0:
                return False, 'Missing files: {}\nRedundant files: {}'.format(missing, redundant)

            for index, (true_item, req_item) in enumerate(zip(true_safe, req_safe)):
                if true_item != req_item:
                    return False, 'Wrong order in line {}, {} expected but {} obtained.' \
                                  '\nEntire struct:\n{}'.format(index, true_item, req_item, req_safe)

            return True, "The SAFE structures match!"

        """
        cls.SafeTestCase('L2A_2.06',
                                           AwsProductRequest('S2B_MSIL2A_20180216T102059_N0206_R065_T35VLL_20180216T122'
                                                             '659', safe_format=True, data_folder=cls.INPUT_FOLDER)),
        """

    @classmethod
    def setUpClass(cls):

        cls.test_cases = [cls.SafeTestCase('L1C_02.01',
                                           AwsProductRequest('S2A_OPER_PRD_MSIL1C_PDMC_20151219T140443_R135_V20151219T0'
                                                             '80616_20151219T080616', safe_format=True,
                                                             data_folder=cls.INPUT_FOLDER)),
                          cls.SafeTestCase('L1C_02.04',
                                           AwsProductRequest('S2A_MSIL1C_20170413T104021_N0204_R008_T31SCA_20170413T104'
                                                             '021', safe_format=True, data_folder=cls.INPUT_FOLDER)),
                          cls.SafeTestCase('L1C_02.05',
                                           AwsProductRequest('S2A_MSIL1C_20171002T194231_N0205_R042_T09UWS_20171002T194'
                                                             '234.SAFE', safe_format=True,
                                                             data_folder=cls.INPUT_FOLDER)),
                          ]

        # Uncomment the following only when creating new test cases
        for test_case in cls.test_cases:
            test_case.save_truth()
            test_case.request.save_data()

    def test_safe_struct(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                true_safe = test_case.load_truth()
                req_safe = test_case.get_request_data()
                is_equal, message = self.SafeTestCase.compare_safe_struct(true_safe, req_safe)
                self.assertTrue(is_equal, message)


"""
class TestAwsSafeTile(TestSentinelHub):
    @classmethod
    def setUpClass(cls):
        cls.request = AwsTileRequest(data_folder=cls.OUTPUT_FOLDER, tile='10UEV', bands=['B01', 'B09', 'B10'],
                                     metafiles='metadata,tileInfo', time='2016-01-09', safe_format=True)
        cls.request.save_data(redownload=True)
        cls.filename_list = cls.request.get_filename_list()

    def test_return_type(self):
        self.assertTrue(isinstance(self.filename_list, list), "Expected a list")
        self.assertEqual(len(self.filename_list), 4, "Expected a list of length 4")


class TestAwsSafeProduct(TestSentinelHub):
    @classmethod
    def setUpClass(cls):
        cls.request = AwsProductRequest(data_folder=cls.OUTPUT_FOLDER, bands='B01', safe_format=True,
                                        product_id='S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_'
                                                   '20160103T171947')
        cls.data = cls.request.get_data(redownload=True)

    def test_return_type(self):
        self.assertTrue(isinstance(self.data, list), "Expected a list")
        self.assertEqual(len(self.data), 125, "Expected a list of length 125")


class TestPartialAwsSafeProduct(TestSentinelHub):
    @classmethod
    def setUpClass(cls):
        bands = 'B12'
        metafiles = 'manifest,preview/B02, datastrip/*/metadata '
        tile = 'T1WCV'
        cls.request = AwsProductRequest(data_folder=cls.OUTPUT_FOLDER, bands=bands,
                                        metafiles=metafiles, safe_format=True, tile_list=[tile],
                                        product_id='S2A_MSIL1C_20171010T003621_N0205_R002_T01WCV_20171010T003615')
        cls.data = cls.request.get_data(save_data=True, redownload=True)

    def test_return_type(self):
        self.assertTrue(isinstance(self.data, list), "Expected a list")
        self.assertEqual(len(self.data), 3, "Expected a list of length 3")
"""

if __name__ == '__main__':
    unittest.main()

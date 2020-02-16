"""
Unit tests for download utilities
"""
import unittest
import copy
import os

from sentinelhub import DownloadRequest, MimeType, DownloadClient
from sentinelhub.exceptions import SHRuntimeWarning
from sentinelhub.testing_utils import TestSentinelHub


class TestDownloadRequest(unittest.TestCase):

    def test_general(self):
        data_folder = './data'
        request = DownloadRequest(
            url='www.sentinel-hub.com',
            headers={'Content-Type': MimeType.JSON.get_string()},
            request_type='POST',
            post_values={'test': 'test'},
            data_type='png',
            save_response=True,
            data_folder=data_folder,
            filename=None,
            return_data=True,
            additional_param=True
        )

        self.assertTrue(isinstance(request.get_request_params(include_metadata=True), dict))

        hashed_name = request.get_hashed_name()
        self.assertEqual(hashed_name, '3908682090daba44fca620fc09cc7cfe')

        request_path, response_path = request.get_storage_paths()
        self.assertEqual(request_path, os.path.join(data_folder, hashed_name, 'request.json'))
        self.assertEqual(response_path, os.path.join(data_folder, hashed_name, 'response.png'))

    def test_invalid_request(self):
        request = DownloadRequest(
            save_response=True,
            data_folder=None,
        )

        with self.assertRaises(ValueError):
            request.raise_if_invalid()

    def test_filename_warnings(self):
        request = DownloadRequest(
            save_response=True,
            data_folder='',
            filename='a' * 256 + '.jpg'
        )

        with self.assertWarns(SHRuntimeWarning):
            request.get_storage_paths()


class TestDownloadClient(TestSentinelHub):

    @classmethod
    def setUp(cls):
        super().setUpClass()

        cls.request = DownloadRequest(
            url='https://roda.sentinel-hub.com/sentinel-s2-l1c/tiles/1/C/CV/2017/1/14/0/tileInfo.json',
            headers={'Content-Type': MimeType.JSON.get_string()},
            data_type='json',
            save_response=True,
            data_folder=cls.OUTPUT_FOLDER,
            filename=None,
            return_data=True
        )

    def test_single_download(self):
        client = DownloadClient(redownload=False)

        result = client.download(self.request)

        self.assertTrue(isinstance(result, dict))

        request_path, response_path = self.request.get_storage_paths()
        self.assertTrue(os.path.isfile(request_path))
        self.assertTrue(os.path.isfile(response_path))

    def test_multiple_downloads(self):
        client = DownloadClient(redownload=True, raise_download_errors=False)

        request2 = copy.deepcopy(self.request)
        request2.save_response = False
        request2.return_data = False

        request3 = copy.deepcopy(self.request)
        request3.url += 'invalid'

        with self.assertWarns(SHRuntimeWarning):
            results = client.download([self.request, request2, request3])

        self.assertTrue(isinstance(results, list))
        self.assertEqual(len(results), 3)
        self.assertTrue(results[1] is None and results[2] is None)


if __name__ == "__main__":
    unittest.main()

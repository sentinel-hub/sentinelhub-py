"""
Unit tests for download utilities
"""
import unittest
import os

from sentinelhub import DownloadRequest, MimeType
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


if __name__ == "__main__":
    unittest.main()

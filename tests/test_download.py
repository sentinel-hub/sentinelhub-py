"""
Unit tests for download utilities
"""
import copy
import os

import pytest

from sentinelhub import DownloadRequest, MimeType, DownloadClient
from sentinelhub.exceptions import SHRuntimeWarning


def test_general():
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

    assert isinstance(request.get_request_params(include_metadata=True), dict)

    hashed_name = request.get_hashed_name()
    assert hashed_name == '3908682090daba44fca620fc09cc7cfe'

    request_path, response_path = request.get_storage_paths()
    assert request_path == os.path.join(data_folder, hashed_name, 'request.json')
    assert response_path == os.path.join(data_folder, hashed_name, 'response.png')


def test_invalid_request():
    request = DownloadRequest(
        save_response=True,
        data_folder=None,
    )

    with pytest.raises(ValueError):
        request.raise_if_invalid()


def test_filename_warnings():
    request = DownloadRequest(
        save_response=True,
        data_folder='',
        filename='a' * 256 + '.jpg'
    )

    with pytest.warns(SHRuntimeWarning):
        request.get_storage_paths()


@pytest.fixture(name='download_request')
def download_request_fixture(output_folder):
    return DownloadRequest(
        url='https://roda.sentinel-hub.com/sentinel-s2-l1c/tiles/1/C/CV/2017/1/14/0/tileInfo.json',
        headers={'Content-Type': MimeType.JSON.get_string()},
        data_type='json',
        save_response=True,
        data_folder=output_folder,
        filename=None,
        return_data=True
    )


def test_single_download(download_request):
    client = DownloadClient(redownload=False)

    result = client.download(download_request)

    assert isinstance(result, dict)

    request_path, response_path = download_request.get_storage_paths()
    assert os.path.isfile(request_path)
    assert os.path.isfile(response_path)


def test_multiple_downloads(download_request):
    client = DownloadClient(redownload=True, raise_download_errors=False)

    request2 = copy.deepcopy(download_request)
    request2.save_response = False
    request2.return_data = False

    request3 = copy.deepcopy(download_request)
    request3.url += 'invalid'

    with pytest.warns(SHRuntimeWarning):
        results = client.download([download_request, request2, request3])

    assert isinstance(results, list)
    assert len(results) == 3
    assert results[1] is None and results[2] is None

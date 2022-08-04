"""
Unit tests for download utilities
"""
import copy
import os

import pytest

from sentinelhub import DownloadClient, DownloadRequest, MimeType, write_data
from sentinelhub.download.request import DownloadResponse
from sentinelhub.exceptions import HashedNameCollisionException, SHRuntimeWarning


@pytest.fixture(name="download_request")
def download_request_fixture(output_folder: str) -> DownloadRequest:
    return DownloadRequest(
        url="https://roda.sentinel-hub.com/sentinel-s2-l1c/tiles/1/C/CV/2017/1/14/0/tileInfo.json",
        headers={"Content-Type": MimeType.JSON.get_string()},
        data_type="json",
        save_response=True,
        data_folder=output_folder,
        filename=None,
        return_data=True,
    )


def test_single_download(download_request: DownloadRequest) -> None:
    client = DownloadClient(redownload=False)

    result = client.download(download_request)

    assert isinstance(result, dict)

    request_path, response_path = download_request.get_storage_paths()
    assert os.path.isfile(request_path)
    assert os.path.isfile(response_path)


def test_download_without_decode_data(download_request: DownloadRequest) -> None:
    client = DownloadClient(redownload=False)

    response = client.download(download_request, decode_data=False)
    assert isinstance(response, DownloadResponse)

    responses = client.download([download_request, download_request], decode_data=False)
    assert isinstance(responses, list)
    assert len(responses) == 2
    assert all(isinstance(resp, DownloadResponse) for resp in responses)


def test_download_with_custom_filename(download_request: DownloadRequest) -> None:
    """Making sure that caching works correctly in this case because request dictionary isn't saved."""
    custom_filename = "tile.json"
    download_request.filename = custom_filename

    client = DownloadClient(redownload=False)

    for _ in range(3):
        client.download(download_request)

    request_path, response_path = download_request.get_storage_paths()
    assert request_path is None
    assert response_path.endswith(custom_filename)
    assert os.path.isfile(response_path)


@pytest.mark.parametrize("show_progress", [True, False])
def test_multiple_downloads(download_request: DownloadRequest, show_progress: bool) -> None:
    client = DownloadClient(redownload=True, raise_download_errors=False)

    request2 = copy.deepcopy(download_request)
    request2.save_response = False
    request2.return_data = False

    request3 = copy.deepcopy(download_request)
    request3.url += "invalid"

    with pytest.warns(SHRuntimeWarning):
        results = client.download([download_request, request2, request3], show_progress=show_progress)

    assert isinstance(results, list)
    assert len(results) == 3
    assert results[1] is None and results[2] is None


def test_hash_collision(download_request: DownloadRequest) -> None:
    client = DownloadClient()

    # Give all requests same hash
    download_request.get_hashed_name = lambda: "same_hash"

    request2 = copy.deepcopy(download_request)
    request3 = copy.deepcopy(download_request)
    request3.post_values = {"zero": 0}

    client.download(download_request)
    client.download(request2)

    with pytest.raises(HashedNameCollisionException):
        client.download(request3)

    # Check that there are no issues with re-loading
    request4 = copy.deepcopy(download_request)
    request4.post_values = {"transformed-when-saved": [(1, 2)]}

    request_path, _ = request4.get_storage_paths()
    request_info = request4.get_request_params(include_metadata=True)
    write_data(request_path, request_info, data_format=MimeType.JSON)  # Copied from download client

    # pylint: disable=protected-access
    client._check_cached_request_is_matching(request4, request_path)

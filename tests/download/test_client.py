"""
Unit tests for download utilities
"""
import copy
import os

import pytest

from sentinelhub import DownloadClient, DownloadRequest, MimeType, write_data
from sentinelhub.download.models import DownloadResponse
from sentinelhub.exceptions import HashedNameCollisionException, SHRuntimeWarning


@pytest.fixture(name="download_request")
def download_request_fixture(output_folder: str) -> DownloadRequest:
    return DownloadRequest(
        url="https://roda.sentinel-hub.com/sentinel-s2-l1c/tiles/1/C/CV/2017/1/14/0/tileInfo.json",
        headers={"Content-Type": MimeType.JSON.get_string()},
        data_type=MimeType.JSON,
        save_response=True,
        data_folder=output_folder,
        filename=None,
        return_data=True,
    )


@pytest.mark.parametrize("num_requests", [0, 1, 2])
@pytest.mark.parametrize("decode", [True, False])
def test_download_return_values(num_requests: int, decode: bool, download_request: DownloadRequest) -> None:
    client = DownloadClient(redownload=False)
    requests = [copy.copy(download_request) for _ in range(num_requests)]

    results = client.download(requests, decode_data=decode)

    expected_element_type = dict if decode else DownloadResponse
    assert isinstance(results, list) and len(results) == len(requests)
    assert all(isinstance(result, expected_element_type) for result in results)

    for req in requests:
        request_path, response_path = req.get_storage_paths()
        assert request_path is not None and os.path.isfile(request_path)
        assert response_path is not None and os.path.isfile(response_path)


def test_download_with_custom_filename(download_request: DownloadRequest) -> None:
    """Making sure that caching works correctly in this case because request dictionary isn't saved."""
    custom_filename = "tile.json"
    download_request.filename = custom_filename

    client = DownloadClient(redownload=False)

    for _ in range(3):
        client.download([download_request])

    request_path, response_path = download_request.get_storage_paths()
    assert request_path is None
    assert response_path.endswith(custom_filename)
    assert os.path.isfile(response_path)


@pytest.mark.parametrize("show_progress", [True, False])
def test_download_with_different_options(download_request: DownloadRequest, show_progress: bool) -> None:
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

    structurally_same_request = copy.deepcopy(download_request)
    structurally_different_request = copy.deepcopy(download_request)
    structurally_different_request.post_values = {"zero": 0}

    client.download([download_request])
    client.download([structurally_same_request])

    with pytest.raises(HashedNameCollisionException):
        client.download([structurally_different_request])


def test_check_cached_request_is_matching(download_request: DownloadRequest) -> None:
    """Checks that when the request is saved (and loaded) the method correctly recognizes it's matching.
    Ensures no issues with false detections if the jsonification changes values."""
    client = DownloadClient()

    download_request.post_values = {"transformed-when-saved": [(1, 2)]}

    request_path, _ = download_request.get_storage_paths()
    request_info = download_request.get_request_params(include_metadata=True)
    write_data(request_path, request_info, data_format=MimeType.JSON)  # Copied from download client

    # pylint: disable=protected-access
    client._check_cached_request_is_matching(download_request, request_path)  # noqa: SLF001

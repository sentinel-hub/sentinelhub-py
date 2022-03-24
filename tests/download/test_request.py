"""
Unit tests for DownloadRequest object
"""
import os

import pytest

from sentinelhub import DownloadRequest, MimeType
from sentinelhub.exceptions import SHRuntimeWarning


def test_general():
    data_folder = "./data"
    request = DownloadRequest(
        url="www.sentinel-hub.com",
        headers={"Content-Type": MimeType.JSON.get_string()},
        request_type="POST",
        post_values={"test": "test"},
        data_type="png",
        save_response=True,
        data_folder=data_folder,
        filename=None,
        return_data=True,
        additional_param=True,
    )

    assert isinstance(request.get_request_params(include_metadata=True), dict)

    hashed_name = request.get_hashed_name()
    assert hashed_name == "3908682090daba44fca620fc09cc7cfe"

    request_path, response_path = request.get_storage_paths()
    assert request_path == os.path.join(data_folder, hashed_name, "request.json")
    assert response_path == os.path.join(data_folder, hashed_name, "response.png")


def test_invalid_request():
    request = DownloadRequest(
        save_response=True,
        data_folder=None,
    )

    with pytest.raises(ValueError):
        request.raise_if_invalid()


def test_filename_warnings():
    request = DownloadRequest(save_response=True, data_folder="", filename="a" * 256 + ".jpg")

    with pytest.warns(SHRuntimeWarning):
        request.get_storage_paths()

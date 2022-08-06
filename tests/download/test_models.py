"""
Unit tests for DownloadRequest object
"""
import datetime as dt
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import pytest

from sentinelhub import DownloadRequest, MimeType
from sentinelhub.download.models import DownloadResponse
from sentinelhub.exceptions import SHRuntimeWarning
from sentinelhub.type_utils import JsonDict


def test_download_request() -> None:
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
        extra_params={"param": 42},
    )

    assert isinstance(request.get_request_params(include_metadata=True), dict)

    hashed_name = request.get_hashed_name()
    assert hashed_name == "3908682090daba44fca620fc09cc7cfe"

    request_path, response_path = request.get_storage_paths()
    assert request_path == os.path.join(data_folder, hashed_name, "request.json")
    assert response_path == os.path.join(data_folder, hashed_name, "response.png")


def test_download_request_invalid_request() -> None:
    request = DownloadRequest(
        save_response=True,
        data_folder=None,
    )

    with pytest.raises(ValueError):
        request.raise_if_invalid()


def test_download_request_filename_warnings() -> None:
    request = DownloadRequest(save_response=True, data_folder="", filename="a" * 256 + ".jpg")

    with pytest.warns(SHRuntimeWarning):
        request.get_storage_paths()


@dataclass
class FakeResponse:
    """Mocking requests.response"""

    content: bytes
    headers: JsonDict
    status_code: int
    elapsed: dt.timedelta


def test_download_response(output_folder: str) -> None:
    request = DownloadRequest(
        data_folder=output_folder,
        data_type=MimeType.JSON,
    )

    data = {"foo": "bar"}
    requests_response = FakeResponse(
        content=json.dumps(data).encode(),
        headers={"x": "y"},
        status_code=200,
        elapsed=dt.timedelta(milliseconds=1023.43),
    )

    response = DownloadResponse.from_response(requests_response, request)  # type: ignore[arg-type]
    assert response.request is request
    assert response.content == requests_response.content
    assert response.headers == requests_response.headers
    assert response.status_code == requests_response.status_code
    assert response.elapsed == 1.02343
    assert response.decode() == data

    response.to_local()
    request_path, response_path = request.get_storage_paths()
    assert os.path.exists(request_path)
    assert os.path.exists(response_path)

    new_response = DownloadResponse.from_local(request)
    assert new_response == response and new_response is not response
    assert new_response.decode() == data


@pytest.mark.parametrize(
    "data_type, headers, expected_response_type",
    [
        (MimeType.JSON, None, MimeType.JSON),
        (MimeType.RAW, {"Content-Type": "application/json"}, MimeType.JSON),
        (MimeType.RAW, {"content-type": "application/json"}, MimeType.JSON),
        (MimeType.RAW, {"x": "y"}, MimeType.RAW),
    ],
)
def test_download_response_decoding(
    data_type: MimeType, headers: Optional[JsonDict], expected_response_type: MimeType
) -> None:
    data = {"foo": "bar"}
    response = DownloadResponse(
        request=DownloadRequest(data_type=data_type), content=json.dumps(data).encode(), headers=headers
    )

    assert response.response_type is expected_response_type

    expected_decoded_response = response.content if response.response_type is MimeType.RAW else data
    assert response.decode() == expected_decoded_response


@pytest.mark.parametrize(
    "new_params",
    [
        {"content": b"x"},
        {"status_code": 501, "elapsed": 0.1},
        {},
    ],
)
def test_download_response_derive(new_params: Dict[str, Any]) -> None:
    response = DownloadResponse(request=DownloadRequest(), content=b"", headers={"x": 1})

    derived_response = response.derive(**new_params)
    assert derived_response is not response
    for param in ("request", "content", "headers", "status_code", "elapsed"):
        if param in new_params:
            expected_value = new_params[param]
        else:
            expected_value = getattr(response, param)

        assert getattr(derived_response, param) == expected_value

""" Tests for the Async Process API requests
"""
import datetime as dt

import pytest
import requests
from requests_mock import Mocker

from sentinelhub import CRS, BBox, DataCollection, MimeType, SentinelHubRequest, SHConfig
from sentinelhub.api.process import AsyncProcessRequest, get_async_running_status

pytestmark = pytest.mark.sh_integration


def test_async_process_request_response(requests_mock: Mocker, config: SHConfig) -> None:
    """A test that mocks the response of the async process request."""
    evalscript = "some evalscript"
    time_interval = dt.date(year=2020, month=6, day=1), dt.date(year=2020, month=6, day=10)
    bbox = BBox((14.0, 45.8, 14.2, 46.0), crs=CRS.WGS84)
    request = AsyncProcessRequest(
        evalscript=evalscript,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=DataCollection.SENTINEL2_L1C,
                time_interval=time_interval,
            )
        ],
        responses=[
            SentinelHubRequest.output_response("B02", MimeType.TIFF),
        ],
        delivery=AsyncProcessRequest.s3_specification(url="s3_my_bucket", access_key="foo", secret_access_key="bar"),
        bbox=bbox,
        config=config,
    )

    requests_mock.post("/oauth/token", real_http=True)
    requests_mock.post(
        "/api/v1/async/process",
        [{"json": {"id": "beep", "status": "RUNNING"}}],
    )

    assert request.get_data() == [{"id": "beep", "status": "RUNNING"}]


def test_which_async_requests_running(requests_mock: Mocker, config: SHConfig) -> None:
    running_ids = ["a", "c", "d"]
    nonrunning_ids = ["b", "e"]

    requests_mock.post("/oauth/token", real_http=True)
    for request_id in running_ids:
        requests_mock.get(
            f"/api/v1/async/process/{request_id}",
            [{"json": {"id": request_id, "status": "RUNNING"}}],
        )
    for request_id in nonrunning_ids:
        response_404 = requests.Response()
        response_404.status_code = 404
        requests_mock.get(
            f"/api/v1/async/process/{request_id}", exc=requests.exceptions.HTTPError(response=response_404)
        )

    results = get_async_running_status("abcde", config=config)
    assert results == {"a": True, "c": True, "d": True, "b": False, "e": False}

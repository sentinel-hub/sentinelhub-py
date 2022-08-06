"""
Tests for the module with a special download client for Statistical API
"""
import pytest
from requests_mock import Mocker

from sentinelhub import DownloadFailedException, DownloadRequest, MimeType, SentinelHubStatisticalDownloadClient
from sentinelhub.constants import RequestType


@pytest.fixture(name="download_request")
def request_fixture() -> DownloadRequest:
    return DownloadRequest(
        url="https://mocked.sentinel-hub.com/api/v1/statistics",
        request_type=RequestType.POST,
        data_type=MimeType.JSON,
        use_session=False,
        post_values={"aggregation": {"timeRange": {"from": "2020-01-01", "to": "2020-02-01"}}},
    )


def test_statistical_client_download_per_interval(download_request: DownloadRequest, requests_mock: Mocker) -> None:
    """Mocks Statistical API to test if Statistical client is correctly retrying intervals which have a retriable error
    and replacing data with new data."""
    client = SentinelHubStatisticalDownloadClient(n_interval_retries=2)

    requests_mock.post(
        url="/api/v1/statistics",
        response_list=[
            {
                "json": {
                    "data": [
                        {"interval": {"from": "2020-01-05", "to": "2020-01-05"}, "error": {"type": "EXECUTION_ERROR"}},
                        {"interval": {"from": "2020-01-10", "to": "2020-01-10"}, "error": {"type": "BAD_REQUEST"}},
                        {"interval": {"from": "2020-01-15", "to": "2020-01-15"}},
                    ]
                }
            },
            {
                "json": {
                    "data": [{"interval": {"from": "2020-01-05", "to": "2020-01-05"}, "error": {"type": "TIMEOUT"}}]
                }
            },
            {"json": {"data": [{"interval": {"from": "2020-01-05", "to": "2020-01-05"}, "outputs": 0}]}},
        ],
    )

    data = client.download(download_request)

    assert data == {
        "data": [
            {"interval": {"from": "2020-01-05", "to": "2020-01-05"}, "outputs": 0},
            {"interval": {"from": "2020-01-10", "to": "2020-01-10"}, "error": {"type": "BAD_REQUEST"}},
            {"interval": {"from": "2020-01-15", "to": "2020-01-15"}},
        ]
    }

    assert len(requests_mock.request_history) == 3
    for index, mocked_request in enumerate(requests_mock.request_history):
        interval = (
            {"from": "2020-01-01", "to": "2020-02-01"} if index == 0 else {"from": "2020-01-05", "to": "2020-01-05"}
        )
        assert mocked_request.json() == {"aggregation": {"timeRange": interval}}


def test_statistical_client_runs_out_of_retries(download_request: DownloadRequest, requests_mock: Mocker) -> None:
    client = SentinelHubStatisticalDownloadClient(n_interval_retries=0)

    requests_mock.post(
        url="/api/v1/statistics",
        response_list=[
            {
                "json": {
                    "data": [
                        {"interval": {"from": "2020-01-20", "to": "2020-01-20"}, "error": {"type": "EXECUTION_ERROR"}},
                    ]
                }
            }
        ],
    )

    with pytest.raises(DownloadFailedException) as exception_info:
        client.download(download_request)
        assert str(exception_info.value) == "No more interval retries available, download unsuccessful"

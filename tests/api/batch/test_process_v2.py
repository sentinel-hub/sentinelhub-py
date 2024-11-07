"""
A module that tests an interface for Sentinel Hub Batch processing
"""

import datetime as dt
import itertools as it

import pytest
from requests_mock import Mocker

from sentinelhub import (
    CRS,
    BatchProcessClient,
    BatchProcessRequest,
    BatchRequestStatus,
    BBox,
    DataCollection,
    MimeType,
    SentinelHubRequest,
    SHConfig,
)

pytestmark = pytest.mark.sh_integration


@pytest.fixture(name="batch_client")
def batch_client_fixture(config: SHConfig) -> BatchProcessClient:
    return BatchProcessClient(config=config)


def test_iter_tiling_grids(batch_client: BatchProcessClient) -> None:
    tiling_grids = list(batch_client.iter_tiling_grids())

    assert len(tiling_grids) >= 1
    assert all(isinstance(item, dict) for item in tiling_grids)


def test_single_tiling_grid(batch_client: BatchProcessClient) -> None:
    tiling_grid = batch_client.get_tiling_grid(0)

    assert isinstance(tiling_grid, dict)


def count_batch_get_requests(requests_list: list, request_id: str) -> int:
    return len([r for r in requests_list if r.url.endswith(request_id)])


def test_create_and_run_batch_request(batch_client: BatchProcessClient, requests_mock: Mocker) -> None:
    """A test that mocks creation and execution of a new batch request"""
    evalscript = "some evalscript"
    time_interval = dt.date(year=2020, month=6, day=1), dt.date(year=2020, month=6, day=10)
    bbox = BBox([14.0, 45.8, 14.2, 46.0], crs=CRS.WGS84)
    sentinelhub_request = SentinelHubRequest(
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
        bbox=bbox,
    )

    requests_mock.post("/oauth/token", real_http=True)
    request_id = "mocked-id"
    request_payload = {
        "id": request_id,
        "domainAccountId": 0,
        "request": {
            "input": {
                "bounds": {
                    "bbox": list(bbox),
                    "properties": {"crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"},
                }
            }
        },
        "status": "CREATED",
    }
    requests_mock.post("/api/v2/batch/process", [{"json": request_payload}])

    batch_request = batch_client.create(
        sentinelhub_request,
        input=batch_client.tiling_grid_input(grid_id=1000, resolution=10, buffer_x=50, buffer_y=50),
        output=batch_client.raster_output(delivery="test"),
        description="Test batch job",
    )

    assert isinstance(batch_request, BatchProcessRequest)
    assert batch_request.request_id == request_id
    assert request_id in repr(batch_request)

    endpoints = ["analyse", "start", "stop"]
    full_endpoints = [f"/api/v2/batch/process/{request_id}/{endpoint}" for endpoint in endpoints]
    for full_endpoint in full_endpoints:
        requests_mock.post(full_endpoint, [{"json": ""}])

    # test start analysis
    batch_request.status = BatchRequestStatus.CREATED
    requests_mock.get(
        f"/api/v2/batch/process/{request_id}",
        [
            {"json": {**request_payload, "status": "CREATED"}},
            {"json": {**request_payload, "status": "ANALYSIS_DONE"}},
        ],
    )
    batch_client.start_analysis(batch_request)
    assert count_batch_get_requests(requests_mock.request_history, request_id) == 2

    # test start job
    batch_request.status = BatchRequestStatus.ANALYSIS_DONE
    requests_mock.get(
        f"/api/v2/batch/process/{request_id}",
        [
            {"json": {**request_payload, "status": "ANALYSIS_DONE"}},
            {"json": {**request_payload, "status": "PROCESSING"}},
        ],
    )
    batch_client.start_job(batch_request)
    assert count_batch_get_requests(requests_mock.request_history, request_id) == 4

    # test stop job
    batch_request.status = BatchRequestStatus.PROCESSING
    requests_mock.get(
        f"/api/v2/batch/process/{request_id}",
        [
            {"json": {**request_payload, "status": "PROCESSING"}},
            {"json": {**request_payload, "status": "STOPPED"}},
        ],
    )
    batch_client.stop_job(batch_request)
    assert count_batch_get_requests(requests_mock.request_history, request_id) == 6

    requests_history = [r for r in requests_mock.request_history if not r.url.endswith(request_id)]
    for index, full_endpoint in enumerate(full_endpoints):
        assert requests_history[index - len(full_endpoints)].url.endswith(full_endpoint)


def test_iter_requests(batch_client: BatchProcessClient) -> None:
    batch_requests = list(it.islice(batch_client.iter_requests(), 10))
    assert all(isinstance(request, BatchProcessRequest) for request in batch_requests)

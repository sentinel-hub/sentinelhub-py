"""
A module that tests an interface for Sentinel Hub Batch processing
"""
import datetime as dt
import itertools as it

import pytest

from sentinelhub import SentinelHubBatch, BatchRequest, SentinelHubRequest, DataCollection, BBox, CRS, MimeType
from sentinelhub.constants import ServiceUrl

pytestmark = pytest.mark.sh_integration


@pytest.fixture(name='batch_client')
def batch_client_fixture(config):
    return SentinelHubBatch(config=config)


@pytest.mark.parametrize('base_url', [
    ServiceUrl.MAIN,
    ServiceUrl.USWEST
])
def test_iter_tiling_grids(base_url, config):
    config.sh_base_url = base_url
    batch_client = SentinelHubBatch(config=config)
    tiling_grids = list(batch_client.iter_tiling_grids())

    assert len(tiling_grids) >= 1
    assert all(isinstance(item, dict) for item in tiling_grids)


def test_single_tiling_grid(batch_client):
    tiling_grid = batch_client.get_tiling_grid(0)

    assert isinstance(tiling_grid, dict)


def test_create_and_run_batch_request(batch_client, requests_mock):
    """ A test that mocks creation and execution of a new batch request
    """
    evalscript = 'some evalscript'
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
            SentinelHubRequest.output_response('B02', MimeType.TIFF),
        ],
        bbox=bbox
    )

    requests_mock.post('/oauth/token', real_http=True)
    request_id = 'mocked-id'
    requests_mock.post('/api/v1/batch/process', [{
        'json': {
            'id': request_id,
            'processRequest': {
                'input': {
                    'bounds': {
                        'bbox': list(bbox),
                        'properties': {
                            'crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84'
                        }
                    }
                }
            }
        }
    }])

    batch_request = batch_client.create(
        sentinelhub_request,
        tiling_grid=batch_client.tiling_grid(
            grid_id=1000,
            resolution=10,
            buffer=(50, 50)
        ),
        bucket_name='test',
        description='Test batch job',
    )

    assert isinstance(batch_request, BatchRequest)
    assert batch_request.request_id == request_id
    assert request_id in repr(batch_request)
    assert batch_request.bbox == bbox

    delete_endpoint = f'/api/v1/batch/process/{request_id}'
    requests_mock.delete(delete_endpoint, [{'json': ''}])

    batch_client.delete_request(batch_request)
    requests_mock.request_history[-1].url.endswith(delete_endpoint)

    endpoints = ['analyse', 'start', 'cancel', 'restartpartial']
    full_endpoints = [f'/api/v1/batch/process/{request_id}/{endpoint}' for endpoint in endpoints]
    for full_endpoint in full_endpoints:
        requests_mock.post(full_endpoint, [{'json': ''}])

    batch_client.start_analysis(batch_request)
    batch_client.start_job(batch_request)
    batch_client.cancel_job(batch_request)
    batch_client.restart_job(batch_request)

    for index, full_endpoint in enumerate(full_endpoints):
        assert requests_mock.request_history[index - len(full_endpoints)].url.endswith(full_endpoint)


def test_iter_requests(batch_client):
    batch_requests = list(it.islice(batch_client.iter_requests(), 10))
    assert all(isinstance(request, BatchRequest) for request in batch_requests)

    if batch_requests:
        latest_request = batch_client.get_latest_request()
        assert isinstance(latest_request, BatchRequest)
        assert all(latest_request.created >= request.created for request in batch_requests)

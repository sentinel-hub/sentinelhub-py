"""
A module that tests an interface for Sentinel Hub Batch processing
"""
import datetime as dt
import itertools as it

from sentinelhub import SentinelHubBatch, SentinelHubRequest, DataCollection, BBox, CRS, MimeType


def test_iter_tiling_grids(config):
    tiling_grids = list(SentinelHubBatch.iter_tiling_grids(config=config))

    assert len(tiling_grids) >= 1
    assert all(isinstance(item, dict) for item in tiling_grids)


def test_single_tiling_grid(config):
    tiling_grid = SentinelHubBatch.get_tiling_grid(0, config=config)

    assert isinstance(tiling_grid, dict)


def test_create_and_run_batch_request(config, requests_mock):
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
        bbox=bbox,
        config=config
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

    batch_request = SentinelHubBatch.create(
        sentinelhub_request,
        tiling_grid=SentinelHubBatch.tiling_grid(
            grid_id=1000,
            resolution=10,
            buffer=(50, 50)
        ),
        bucket_name='test',
        description='Test batch job',
        config=config
    )

    assert isinstance(batch_request, SentinelHubBatch)
    assert batch_request.request_id == request_id
    assert batch_request.info['id'] == request_id
    assert request_id in repr(batch_request)
    assert batch_request.bbox == bbox

    delete_endpoint = f'/api/v1/batch/process/{request_id}'
    requests_mock.delete(delete_endpoint, [{'json': ''}])

    batch_request.delete()
    requests_mock.request_history[-1].url.endswith(delete_endpoint)

    endpoints = ['analyse', 'start', 'cancel', 'restartpartial']
    full_endpoints = [f'/api/v1/batch/process/{request_id}/{endpoint}' for endpoint in endpoints]
    for full_endpoint in full_endpoints:
        requests_mock.post(full_endpoint, [{'json': ''}])

    batch_request.start_analysis()
    batch_request.start_job()
    batch_request.cancel_job()
    batch_request.restart_job()

    for index, full_endpoint in enumerate(full_endpoints):
        assert requests_mock.request_history[index - len(full_endpoints)].url.endswith(full_endpoint)


def test_iter_requests(config):
    batch_requests = list(it.islice(SentinelHubBatch.iter_requests(config=config), 10))
    assert all(isinstance(request, SentinelHubBatch) for request in batch_requests)

    if batch_requests:
        latest_request = SentinelHubBatch.get_latest_request(config=config)
        assert isinstance(latest_request, SentinelHubBatch)
        assert all(latest_request.info['created'] >= request.info['created'] for request in batch_requests)

"""
A module that tests an interface for Sentinel Hub Batch processing
"""
import os
import datetime as dt
import itertools as it

import pytest

from sentinelhub import SentinelHubBatch, SentinelHubRequest, DataCollection, BBox, CRS, SHConfig, MimeType


@pytest.fixture(name='config')
def config_fixture():
    config = SHConfig()
    for param in config.get_params():
        env_variable = param.upper()
        if os.environ.get(env_variable):
            setattr(config, param, os.environ.get(env_variable))
    return config


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
    assert batch_request.geometry == bbox

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

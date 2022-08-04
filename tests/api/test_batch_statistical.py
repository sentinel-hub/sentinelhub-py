"""
A module that tests an interface for Sentinel Hub Batch processing
"""
import pytest

from sentinelhub import BatchStatisticalRequest, DataCollection, SentinelHubBatchStatistical, SentinelHubStatistical

pytestmark = pytest.mark.sh_integration


@pytest.fixture(name="statistical_batch_client")
def statistical_batch_client_fixture(config):
    return SentinelHubBatchStatistical(config=config)


def test_create_and_run_batch_request(statistical_batch_client, requests_mock):
    """A test that mocks creation and execution of a new batch request"""
    rgb_evalscript = "some evalscript"

    aggregation = SentinelHubStatistical.aggregation(
        evalscript=rgb_evalscript,
        time_interval=("2020-06-07", "2020-06-13"),
        aggregation_interval="P1D",
        size=(631, 1047),
    )
    input_data = [SentinelHubStatistical.input_data(DataCollection.SENTINEL2_L1C, maxcc=0.8)]
    calculations = {"ndvi": {"histograms": {"default": {"nBins": 20, "lowEdge": -1.0, "highEdge": 1.0}}}}

    input_features = {"s3": {"url": "s3://path/to/gpkg", "accessKey": "", "secretAccessKey": ""}}
    output = {"s3": {"url": "s3://path/to/output/folder", "accessKey": "", "secretAccessKey": ""}}

    requests_mock.post("/oauth/token", real_http=True)
    request_id = "mocked-id"
    requests_mock.post(
        "/api/v1/statistics/batch",
        [
            {
                "json": {
                    "id": request_id,
                    "request": {},
                    "completion_percentage": 0,
                    "status": "CREATED",
                }
            }
        ],
    )

    batch_request = statistical_batch_client.create(
        input_data=input_data,
        input_features=input_features,
        aggregation=aggregation,
        calculations=calculations,
        output=output,
    )

    assert isinstance(batch_request, BatchStatisticalRequest)
    assert batch_request.request_id == request_id
    assert request_id in repr(batch_request)

    endpoints = ["analyse", "start", "cancel"]
    full_endpoints = [f"/api/v1/statistics/batch/{request_id}/{endpoint}" for endpoint in endpoints]
    for full_endpoint in full_endpoints:
        requests_mock.post(full_endpoint, [{"json": ""}])

    statistical_batch_client.start_analysis(batch_request)
    statistical_batch_client.start_job(batch_request)
    statistical_batch_client.cancel_job(batch_request)

    for index, full_endpoint in enumerate(full_endpoints):
        assert requests_mock.request_history[index - len(full_endpoints)].url.endswith(full_endpoint)

"""
Tests for AWS batch module
"""
import json
from enum import Enum
from typing import Optional, Sequence

import boto3
import pytest
from moto import mock_s3
from pytest_mock import MockerFixture

from sentinelhub import BatchRequestStatus, BatchStatisticalRequest, SHConfig
from sentinelhub.api.batch.statistical import BatchStatisticalRequestType
from sentinelhub.aws import AwsBatchResults
from sentinelhub.type_utils import JsonDict


class BatchInputType(Enum):
    ID = "batch_id"
    DICT = "batch_dict"
    OBJECT = "batch_object"


@mock_s3
def _create_mocked_bucket_and_upload_data(bucket_name: str, paths: Sequence[str], data: Sequence[JsonDict]) -> str:
    """Creates a new empty mocked s3 bucket. If one such bucket already exists it deletes it first.

    Note: Creating a bucket and uploading data could be 2 separate methods but then moto mocking somehow fails.
    """
    s3resource = boto3.resource("s3", region_name="eu-central-1")

    bucket = s3resource.Bucket(bucket_name)

    if bucket.creation_date:  # If bucket already exists
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()

    s3resource.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-central-1"})

    for path, data_dict in zip(paths, data):
        bucket.put_object(Key=path, Body=json.dumps(data_dict).encode())

    return bucket_name


@mock_s3
@pytest.mark.parametrize("batch_input_type", list(BatchInputType))
@pytest.mark.parametrize("use_feature_ids", [True, False])
@pytest.mark.parametrize("config, show_progress", [(None, False), (SHConfig(), True)])
def test_aws_batch_results(
    batch_input_type: BatchInputType,
    use_feature_ids: bool,
    config: Optional[SHConfig],
    show_progress: bool,
    mocker: MockerFixture,
) -> None:
    """This test mocks an S3 bucket and data on it. Then it runs AwsBatchResults and downloads results from it."""
    bucket_name = "mocked-test-bucket"
    batch_id = "fake-batch-id"
    prefix = "path/to/outputs/"
    item_num = 3

    feature_ids = list(range(item_num))
    paths = [f"{prefix}{batch_id}/{feature_id}.json" for feature_id in feature_ids]
    data = [{"foo": "bar"} for _ in range(item_num)]

    _create_mocked_bucket_and_upload_data(bucket_name, paths, data)

    batch_request = BatchStatisticalRequest(
        request_id=batch_id,
        completion_percentage=100,
        status=BatchRequestStatus.DONE,
        request={"output": {"s3": {"url": f"s3://{bucket_name}/{prefix}"}}},
    )

    batch_input: BatchStatisticalRequestType
    if batch_input_type is BatchInputType.ID:
        batch_input = batch_id
        batch_mock = mocker.patch("sentinelhub.SentinelHubBatchStatistical.get_request")
        batch_mock.side_effect = [batch_request]
    elif batch_input_type is BatchInputType.DICT:
        batch_input = batch_request.to_dict()
    else:
        batch_input = batch_request

    feature_ids_input = feature_ids if use_feature_ids else None
    results = AwsBatchResults(batch_input, feature_ids=feature_ids_input, config=config)
    downloaded_data = results.get_data(show_progress=show_progress)

    assert downloaded_data == data

"""
Module implementing utilities for collecting data, produced with Sentinel Hub Statistical Batch API, from an S3 bucket.
"""
from typing import List, Optional, Sequence, Union

from ..api.batch.statistical import BatchStatisticalRequest, BatchStatisticalRequestType, SentinelHubBatchStatistical
from ..base import DataRequest
from ..config import SHConfig
from ..constants import MimeType
from ..download.models import DownloadRequest
from ..exceptions import deprecated_class
from .client import AwsDownloadClient


class AwsBatchStatisticalResults(DataRequest):
    """A utility class for downloading results of Batch Statistical API from an S3 bucket."""

    def __init__(
        self,
        batch_request: BatchStatisticalRequestType,
        *,
        feature_ids: Optional[Sequence[Union[str, int]]] = None,
        data_folder: Optional[str] = None,
        config: Optional[SHConfig] = None,
    ):
        """
        :param batch_request: Info about a batch request - either an instance of `BatchStatisticalRequest` or a
            batch ID or a raw payload of the batch response.
        :param feature_ids: A list of feature IDs of saved results on the bucket. If provided it will download only
            these results. If not provided it will collect the names of all JSON files from results folder on the
            bucket and download all of them. Note that it is recommended that you provide this parameter otherwise this
            class will have to make additional requests to the S3 bucket in order to list all features from the folder.
        :param data_folder: Directory to which the files should be saved.
        :param config: A config object that contains AWS credentials to access the S3 bucket with results.
        """
        self.batch_request = self._parse_batch_request(batch_request, config)
        self.feature_ids = feature_ids

        super().__init__(AwsDownloadClient, data_folder=data_folder, config=config)

    @staticmethod
    def _parse_batch_request(
        batch_request: BatchStatisticalRequestType, config: Optional[SHConfig]
    ) -> BatchStatisticalRequest:
        """In case a batch request is not defined with an instance of `BatchStatisticalRequest` it will make sure that
        such an instance is created."""
        if isinstance(batch_request, BatchStatisticalRequest):
            return batch_request

        if isinstance(batch_request, dict):
            return BatchStatisticalRequest.from_dict(batch_request)

        batch_client = SentinelHubBatchStatistical(config=config)
        return batch_client.get_request(batch_request)

    def create_request(self) -> None:
        """Creates a list of download requests."""
        base_s3_path = self.batch_request.request["output"]["s3"]["url"].rstrip("/")
        s3_path = f"{base_s3_path}/{self.batch_request.request_id}/"

        filenames = self._get_filenames(s3_path)

        self.download_list = [
            DownloadRequest(
                url=f"{s3_path}{filename}", data_folder=self.data_folder, data_type=MimeType.JSON, filename=filename
            )
            for filename in filenames
        ]

    def _get_filenames(self, s3_path: str) -> List[str]:
        """Creates a list of JSON filenames from given feature ids or from given S3 path if feature ids are not
        provided. In case if it has to collect them from S3 path it makes sure not to collect any data from any
        subfolder in the path."""
        if self.feature_ids is not None:
            return [f"{feature_id}.json" for feature_id in self.feature_ids]

        filenames: List[str] = []

        s3_client = AwsDownloadClient.get_s3_client(self.config)
        _, _, bucket_name, url_key = s3_path.split("/", 3)

        paginator = s3_client.get_paginator("list_objects")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=url_key):
            for item in page["Contents"]:
                key_path = item["Key"]
                key_name = key_path.rsplit("/", 1)[1]

                if key_name.endswith(".json") and key_path == f"{url_key}{key_name}":
                    filenames.append(key_name)

        return filenames


@deprecated_class(message_suffix="It has been renamed to `AwsBatchStatisticalResults`.")
class AwsBatchResults(AwsBatchStatisticalResults):
    """Deprecated version of `AwsBatchStatisticalResults`."""

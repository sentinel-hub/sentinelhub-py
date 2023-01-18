"""
Module implementing an interface with
`Sentinel Hub Batch Processing API <https://docs.sentinel-hub.com/api/latest/api/batch-statistical/>`__.
"""
import datetime as dt
import logging
import sys
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence, Union

from dataclasses_json import CatchAll, LetterCase, Undefined
from dataclasses_json import config as dataclass_config
from dataclasses_json import dataclass_json

from ...types import Json, JsonDict
from ..base_request import InputDataDict
from ..statistical import SentinelHubStatistical
from ..utils import datetime_config, enum_config, remove_undefined
from .base import BaseBatchClient, BaseBatchRequest, BatchRequestStatus, BatchUserAction

if sys.version_info < (3, 11):
    from typing_extensions import NotRequired, TypedDict
else:
    from typing import NotRequired, TypedDict  # pylint: disable=ungrouped-imports

LOGGER = logging.getLogger(__name__)

BatchStatisticalRequestType = Union[str, dict, "BatchStatisticalRequest"]


class S3Specification(TypedDict):
    """Specification of a S3 path."""

    url: str
    accessKey: str
    secretAccessKey: str
    region: NotRequired[str]


class AccessSpecification(TypedDict):
    """Specification of a S3 input or output."""

    s3: S3Specification


class SentinelHubBatchStatistical(BaseBatchClient["BatchStatisticalRequest"]):
    """An interface class for Sentinel Hub Batch Statistical API

    Check `Batch Statistical API <https://docs.sentinel-hub.com/api/latest/reference/#tag/batch_statistical>`__ for more
    information.
    """

    @staticmethod
    def _get_service_url(base_url: str) -> str:
        """Provides URL to Batch Statistical API"""
        return f"{base_url}/api/v1/statistics/batch"

    def create(
        self,
        *,
        input_features: AccessSpecification,
        input_data: Sequence[Union[JsonDict, InputDataDict]],
        aggregation: JsonDict,
        calculations: Optional[JsonDict],
        output: AccessSpecification,
        **kwargs: Any,
    ) -> "BatchStatisticalRequest":
        """Create a new batch statistical request

        `Batch Statistical API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/createNewBatchStatisticsRequest>`__
        """

        # Data filter has to be set to {} if not provided. Ensure we do not mutate original data.
        requested_data = list(input_data)
        for i, data_request_dict in enumerate(requested_data):
            if "dataFilter" not in data_request_dict:
                requested_data[i] = {"dataFilter": {}, **data_request_dict}

        payload = {
            "input": {"features": input_features, "data": requested_data},
            "aggregation": aggregation,
            "calculations": calculations,
            "output": output,
            **kwargs,
        }
        payload = remove_undefined(payload)

        url = self.service_url
        request_info = self.client.get_json_dict(url, post_values=payload, use_session=True)

        return BatchStatisticalRequest.from_dict(request_info)

    def create_from_request(
        self,
        statistical_request: SentinelHubStatistical,
        input_features: AccessSpecification,
        output: AccessSpecification,
        **kwargs: Any,
    ) -> "BatchStatisticalRequest":
        """Create a new batch statistical request from an existing statistical request.

        :param statistical_request: A Sentinel Hub Statistical request.
        :param input_features: A dictionary describing the S3 path and credentials to access the input GeoPackage.
        :param output: A dictionary describing the S3 path and credentials to access the output folder.
        :param kwargs: Any other arguments to be added to a dictionary of parameters
        :returns: A Batch Statistical request with the same calculations and aggregations but using geometries
            specified in the input GeoPackage.
        """

        return self.create(
            input_features=input_features,
            input_data=statistical_request.payload["input"]["data"],
            aggregation=statistical_request.payload["aggregation"],
            calculations=statistical_request.payload["calculations"],
            output=output,
            **kwargs,
        )

    @staticmethod
    def s3_specification(
        url: str, access_key: str, secret_access_key: str, region: Optional[str] = None
    ) -> AccessSpecification:
        """A helper method to build a dictionary used for specifying S3 paths

        :param url: A URL pointing to an S3 bucket or an object in an S3 bucket.
        :param access_key: AWS access key that allows programmatic access to the S3 bucket specified in the `url` field.
        :param secret_access_key: AWS secret access key which must correspond to the AWS access key.
        :param region: The region where the S3 bucket is located. If omitted, the region of the Sentinel Hub deployment
            that the request is submitted to is assumed.
        :return: A dictionary of S3 specifications used by the Batch Statistical API
        """
        s3_access: S3Specification = {"url": url, "accessKey": access_key, "secretAccessKey": secret_access_key}
        if region is not None:
            s3_access["region"] = region
        return {"s3": s3_access}

    def get_request(self, batch_request: BatchStatisticalRequestType) -> "BatchStatisticalRequest":
        """Collects information about a single batch request

        `Batch Statistical API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/getSingleBatchStatisticalRequestById>`__

        :return: Batch request info
        """
        request_id = self._parse_request_id(batch_request)
        request_info = self.client.get_json_dict(url=self._get_processing_url(request_id), use_session=True)
        return BatchStatisticalRequest.from_dict(request_info)

    def get_status(self, batch_request: BatchStatisticalRequestType) -> JsonDict:
        """Collects information about a status of a request

        `Batch Statistical API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/batchStatisticalGetStatus>`__

        :return: Batch request status dictionary
        """
        request_id = self._parse_request_id(batch_request)
        endpoint_url = f"{self._get_processing_url(request_id)}/status"
        request_info = self.client.get_json_dict(url=endpoint_url, use_session=True)
        return request_info

    def start_analysis(self, batch_request: BatchStatisticalRequestType) -> Json:
        """Starts analysis of a batch job request

        `Batch Statistical API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/batchStatisticalAnalyse>`__

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        """
        return self._call_job(batch_request, "analyse")

    def start_job(self, batch_request: BatchStatisticalRequestType) -> Json:
        """Starts running a batch job

        `Batch Statistical API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/batchStartStatisticalRequest>`__

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        """
        return self._call_job(batch_request, "start")

    def cancel_job(self, batch_request: BatchStatisticalRequestType) -> Json:
        """Cancels a batch job

        `Batch Statistical API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/batchCancelStatisticalRequest>`__

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        """
        return self._call_job(batch_request, "cancel")


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass(repr=False)
class BatchStatisticalRequest(BaseBatchRequest):  # pylint: disable=abstract-method
    """A dataclass object that holds information about a batch statistical request"""

    # dataclass_json doesn't handle parameter inheritance correctly
    # pylint: disable=duplicate-code

    request_id: str = field(metadata=dataclass_config(field_name="id"))
    completion_percentage: float
    request: dict
    status: BatchRequestStatus = field(metadata=enum_config(BatchRequestStatus))
    user_id: Optional[str] = None
    created: Optional[dt.datetime] = field(metadata=datetime_config, default=None)
    cost_pu: Optional[float] = field(metadata=dataclass_config(field_name="costPU"), default=None)
    user_action: Optional[BatchUserAction] = field(metadata=enum_config(BatchUserAction), default=None)
    user_action_updated: Optional[str] = field(metadata=datetime_config, default=None)
    error: Optional[str] = None
    other_data: CatchAll = field(default_factory=dict)

    _REPR_PARAM_NAMES = (
        "request_id",
        "created",
        "status",
        "user_action",
        "cost_pu",
        "error",
        "completion_percentage",
    )

"""
Module implementing an interface with
`Sentinel Hub Batch Processing API <https://docs.sentinel-hub.com/api/latest/api/batch-statistical/>`__.
"""

# ruff: noqa: FA100
# do not use `from __future__ import annotations`, it clashes with `dataclass_json`
import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence, Union

from dataclasses_json import CatchAll, LetterCase, Undefined, dataclass_json
from dataclasses_json import config as dataclass_config
from typing_extensions import deprecated

from ...exceptions import SHDeprecationWarning
from ...types import Json, JsonDict
from ..base_request import InputDataDict
from ..statistical import SentinelHubStatistical
from ..utils import AccessSpecification, datetime_config, enum_config, remove_undefined, s3_specification
from .base import BaseBatchClient, BaseBatchRequest, BatchRequestStatus, BatchUserAction

LOGGER = logging.getLogger(__name__)

BatchStatisticalRequestType = Union[str, dict, "BatchStatisticalRequest"]


class SentinelHubBatchStatistical(BaseBatchClient["BatchStatisticalRequest"]):
    """An interface class for Sentinel Hub Batch Statistical API

    Check `Batch Statistical API <https://docs.sentinel-hub.com/api/latest/reference/#tag/batch_statistical>`__ for more
    information.
    """

    s3_specification = s3_specification

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
        payload = {
            "input": {"features": input_features, "data": list(input_data)},
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
        return self.client.get_json_dict(url=endpoint_url, use_session=True)

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

    @deprecated("The method `cancel_job` has been replaced with use `stop_job`.", category=SHDeprecationWarning)
    def cancel_job(self, batch_request: BatchStatisticalRequestType) -> Json:
        """Cancels a batch job

        `Batch Statistical API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/batchCancelStatisticalRequest>`__

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        """
        return self._call_job(batch_request, "cancel")

    def stop_job(self, batch_request: BatchStatisticalRequestType) -> Json:
        """Stop a batch job

        `Batch Statistical API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#tag/batch_statistical/operation/batchStopStatisticalRequest>`__

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        """
        return self._call_job(batch_request, "stop")


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

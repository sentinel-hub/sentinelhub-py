"""
Module implementing an interface with
`Batch Process V2 <https://docs.sentinel-hub.com/api/latest/api/batchv2/>`__.
"""

# ruff: noqa: FA100
# do not use `from __future__ import annotations`, it clashes with `dataclass_json`
import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, Optional, Union

from dataclasses_json import CatchAll, LetterCase, Undefined, dataclass_json
from dataclasses_json import config as dataclass_config
from typing_extensions import Literal

from ...constants import RequestType
from ...types import Json, JsonDict
from ..base import SentinelHubFeatureIterator
from ..process import SentinelHubRequest
from ..utils import AccessSpecification, datetime_config, enum_config, remove_undefined, s3_specification
from .base import BaseBatchClient, BaseBatchRequest, BatchRequestStatus, BatchUserAction, StoppedStatusReason

LOGGER = logging.getLogger(__name__)

BatchRequestType = Union[str, dict, "BatchProcessRequest"]


class BatchProcessClient(BaseBatchClient):
    """An interface class for Sentinel Hub Batch API version 2.

    `Batch Process API <https://docs.sentinel-hub.com/api/latest/api/batchv2/>`__
    """

    s3_specification = staticmethod(s3_specification)

    # pylint: disable=too-many-public-methods
    @staticmethod
    def _get_service_url(base_url: str) -> str:
        """Provides URL to Catalog API"""
        return f"{base_url}/api/v2/batch"

    def _get_processing_url(self, request_id: Optional[str] = None) -> str:
        """Creates a URL for process endpoint"""
        url = f"{self.service_url}/process"
        if request_id is None:
            return url
        return f"{url}/{request_id}"

    def create(
        self,
        process_request: Union[SentinelHubRequest, JsonDict],
        input: Dict[str, Any],  # noqa: A002  #pylint: disable=redefined-builtin
        output: Dict[str, Any],
        instance_type: Literal["normal", "large"] = "normal",
        description: Optional[str] = None,
        **kwargs: Any,
    ) -> "BatchProcessRequest":
        """Create a new batch request

        `Batch Process V2 <https://docs.sentinel-hub.com/api/latest/api/batchv2/>`__

        :param process_request: An instance of SentinelHubRequest class containing all request parameters.
            Alternatively, it can also be just a payload dictionary for Process API request
        :param input: A dictionary with input parameters. It can be built with `tiling_grid_input` or `geopackage_input`
            methods.
        :param output: A dictionary with output parameters. It can be built with `raster_output` or `zarr_output`
            methods.
        :param instance_type": Specifies which size of instances to use for the request.
        :param description: A description of a batch request
        :param kwargs: Any other arguments to be added to a dictionary of parameters.
        """

        if isinstance(process_request, SentinelHubRequest):
            request_dict = process_request.download_list[0].post_values
        else:
            request_dict = process_request

        if not isinstance(request_dict, dict):
            raise ValueError(
                "Parameter sentinelhub_request should be an instance of SentinelHubRequest or a "
                "dictionary with a request payload"
            )

        payload = remove_undefined(
            {
                "processRequest": request_dict,
                "input": input,
                "output": output,
                "instance_type": instance_type,
                "description": description,
                **kwargs,
            }
        )

        request_info = self.client.get_json_dict(self._get_processing_url(), post_values=payload, use_session=True)

        return BatchProcessRequest.from_dict(request_info)

    @staticmethod
    def geopackage_input(geopackage_specification: AccessSpecification) -> JsonDict:
        """A helper method to build a suitable dictionary for the `input` field.

        :param geopackage_specification: A specification of the S3 path for the Geopackage. Can be built using the
            `s3_specification` helper method.
        """
        return {"type": "geopackage", "features": geopackage_specification}

    @staticmethod
    def tiling_grid_input(
        grid_id: int, resolution: float, buffer_x: Optional[int] = None, buffer_y: Optional[int] = None, **kwargs: Any
    ) -> JsonDict:
        """A helper method to build a dictionary with tiling grid parameters for the `input` field.

        :param grid_id: An ID of a tiling grid
        :param resolution: A grid resolution
        :param buffer_x: Will expand each output tile horizontally (left and right) by specified number of pixels.
        :param buffer_y: Will expand each output tile vertically (up and down) by specified number of pixels.
        :param kwargs: Any other arguments to be added to a dictionary of parameters
        """
        return remove_undefined(
            {
                "type": "tiling-grid",
                "id": grid_id,
                "resolution": resolution,
                "bufferX": buffer_x,
                "bufferY": buffer_y,
                **kwargs,
            }
        )

    @staticmethod
    def raster_output(
        delivery: AccessSpecification,
        *,
        overwrite: Optional[bool] = None,
        skip_existing: Optional[bool] = None,
        cog_output: Optional[bool] = None,
        cog_parameters: Optional[Dict[str, Any]] = None,
        create_collection: Optional[bool] = None,
        collection_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """A helper method to build a dictionary specifying raster output

        :param delivery: An S3 access specification containing a path or a template on an s3 bucket where to store
            results. You can use the `s3_specification` method for construction. For more information on templates see
            documentation.
        :param overwrite: A flag specifying if a request should overwrite existing outputs without failing
        :param skip_existing: A flag specifying if existing outputs should be overwritten
        :param cog_output: A flag specifying if outputs should be written in COGs (cloud-optimized GeoTIFFs) or
            normal GeoTIFFs
        :param cog_parameters: A dictionary specifying COG creation parameters. See documentation for more info.
        :param create_collection: If True the results will be written in COGs and a batch collection will be created
        :param collection_id: If True results will be added to an existing collection
        :param kwargs: Any other arguments to be added to a dictionary of parameters
        """
        return remove_undefined(
            {
                "type": "raster",
                "delivery": delivery,
                "overwrite": overwrite,
                "skipExisting": skip_existing,
                "cogOutput": cog_output,
                "cogParameters": cog_parameters,
                "createCollection": create_collection,
                "collectionId": collection_id,
                **kwargs,
            }
        )

    @staticmethod
    def zarr_output(
        delivery: AccessSpecification,
        *,
        group: Optional[Dict[str, Any]] = None,
        array_parameters: Optional[Dict[str, Any]] = None,
        array_overrides: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> JsonDict:
        """A helper method to build a dictionary specifying Zarr output. See documentation for more information on
        each parameter.

        `Batch Process V2 <https://docs.sentinel-hub.com/api/latest/api/batchv2/>`__

        :param delivery: An S3 access specification containing a path or a template on an s3 bucket where to store
            results. You can use the `s3_specification` method for construction. For more information on templates see
            documentation.
        :param group: Zarr group level parameters
        :param array_parameters: Parameters that will be used for all output arrays, except where overriden with
            `array_overrides`. Required unless `array_overrides` includes all required fields for all output arrays.
        :param array_overrides: Overrides the values of `array_arameters` for individual arrays.
        :param kwargs: Any other arguments to be added to a dictionary of parameters
        """
        return remove_undefined(
            {
                "type": "zarr",
                "delivery": delivery,
                "group": group,
                "arrayParameter": array_parameters,
                "arrayOverrides": array_overrides,
                **kwargs,
            }
        )

    def iter_requests(
        self, user_id: Optional[str] = None, search: Optional[str] = None, sort: Optional[str] = None, **kwargs: Any
    ) -> Iterator["BatchProcessRequest"]:
        """Iterate existing batch requests

        `Batch Process V2 <https://docs.sentinel-hub.com/api/latest/api/batchv2/>`__

        :param user_id: Filter requests by a user id who defined a request
        :param search: A search query to filter requests
        :param sort: A sort query
        :param kwargs: Any additional parameters to include in a request query
        :return: An iterator over existing batch requests
        """
        params = remove_undefined({"userid": user_id, "search": search, "sort": sort, **kwargs})
        feature_iterator = SentinelHubFeatureIterator(
            client=self.client, url=self._get_processing_url(), params=params, exception_message="No requests found"
        )
        for request_info in feature_iterator:
            yield BatchProcessRequest.from_dict(request_info)

    def get_request(self, batch_request: BatchRequestType) -> "BatchProcessRequest":
        """Collects information about a single batch request.

        `Batch Process V2 <https://docs.sentinel-hub.com/api/latest/api/batchv2/>`__
        """
        request_id = self._parse_request_id(batch_request)
        request_info = self.client.get_json_dict(url=self._get_processing_url(request_id), use_session=True)
        return BatchProcessRequest.from_dict(request_info)

    def update_request(self, batch_request: BatchRequestType, description: str) -> Json:
        """Update certain batch job request parameters. Can only update requests that are not currently being processed.

        `Batch Process V2 <https://docs.sentinel-hub.com/api/latest/api/batchv2/>`__

        :param batch_request: Batch request ID, a dictionary containing an "ID" field, or a BatchProcessRequest.
        :param description: A description of a batch request to be updated.
        """
        request_id = self._parse_request_id(batch_request)

        return self.client.get_json(
            url=self._get_processing_url(request_id),
            post_values={"description": description},
            request_type=RequestType.PUT,
            use_session=True,
        )

    def start_analysis(self, batch_request: BatchRequestType) -> Json:
        """Starts analysis of a batch job request

        :param batch_request: Batch request ID, a dictionary containing an "ID" field, or a BatchProcessRequest.
        """
        return self._call_job(batch_request, "analyse")

    def start_job(self, batch_request: BatchRequestType) -> Json:
        """Starts running a batch job

        :param batch_request: Batch request ID, a dictionary containing an "ID" field, or a BatchProcessRequest.
        """
        return self._call_job(batch_request, "start")

    def stop_job(self, batch_request: BatchRequestType) -> Json:
        """Stops a batch job

        :param batch_request: Batch request ID, a dictionary containing an "ID" field, or a BatchProcessRequest.
        """
        return self._call_job(batch_request, "stop")

    def iter_tiling_grids(self, **kwargs: Any) -> SentinelHubFeatureIterator:
        """An iterator over tiling grids

        `Batch Process V2 <https://docs.sentinel-hub.com/api/latest/api/batchv2/>`__

        :param kwargs: Any other request query parameters
        :return: An iterator over tiling grid definitions
        """
        return SentinelHubFeatureIterator(
            client=self.client,
            url=f"{self.service_url}/tilinggrids",
            params=remove_undefined(kwargs),
            exception_message="Failed to obtain information about available tiling grids",
        )

    def get_tiling_grid(self, grid_id: int) -> JsonDict:
        """Provides a single tiling grid

        `Batch Process V2 <https://docs.sentinel-hub.com/api/latest/api/batchv2/>`__

        :param grid_id: An ID of a requested tiling grid
        :return: A tiling grid definition
        """
        url = f"{self.service_url}/tilinggrids/{grid_id}"
        return self.client.get_json_dict(url=url, use_session=True)


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass(repr=False)
class BatchProcessRequest(BaseBatchRequest):  # pylint: disable=abstract-method
    """A dataclass object that holds information about a batch request"""

    # dataclass_json doesn't handle parameter inheritance correctly
    # pylint: disable=invalid-name

    request_id: str = field(metadata=dataclass_config(field_name="id"))
    request: dict
    domain_account_id: str
    status: BatchRequestStatus = field(metadata=enum_config(BatchRequestStatus))
    error: Optional[str] = None
    user_action: Optional[BatchUserAction] = field(metadata=enum_config(BatchUserAction), default=None)
    user_action_updated: Optional[dt.datetime] = field(metadata=datetime_config, default=None)
    created: Optional[dt.datetime] = field(metadata=datetime_config, default=None)
    completion_percentage: float = 0
    last_updated: Optional[dt.datetime] = field(metadata=datetime_config, default=None)
    cost_PU: Optional[float] = field(metadata=dataclass_config(field_name="costPU"), default=None)  # noqa: N815
    stopped_status_reason: Optional[StoppedStatusReason] = field(
        metadata=enum_config(StoppedStatusReason), default=None
    )

    other_data: CatchAll = field(default_factory=dict)

    _REPR_PARAM_NAMES = ("request_id", "created", "status", "completion_percentage", "user_action", "cost_PU")

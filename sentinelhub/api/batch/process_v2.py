"""
Module implementing an interface with
`Sentinel Hub Batch Processing API <https://docs.sentinel-hub.com/api/latest/api/batch/>`__.
"""

# ruff: noqa: FA100
# do not use `from __future__ import annotations`, it clashes with `dataclass_json`
import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from dataclasses_json import CatchAll, LetterCase, Undefined, dataclass_json
from dataclasses_json import config as dataclass_config
from typing_extensions import Literal

from ...constants import RequestType
from ...geometry import CRS, BBox, Geometry
from ...types import Json, JsonDict
from ..base import SentinelHubFeatureIterator
from ..process import SentinelHubRequest
from ..utils import datetime_config, enum_config, remove_undefined, s3_specification
from .base import BaseBatchClient, BaseBatchRequest, BatchRequestStatus, BatchUserAction

LOGGER = logging.getLogger(__name__)

BatchRequestType = Union[str, dict, "BatchRequest"]


class SentinelHubBatch(BaseBatchClient):
    """An interface class for Sentinel Hub Batch API version 2."""

    s3_specification = s3_specification

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
        input: Dict[str, Any],  # noqa: A002
        output: Optional[Dict[str, Any]] = None,
        instance_type: Literal["normal", "large"] = "normal",
        description: Optional[str] = None,
        **kwargs: Any,
    ) -> "BatchRequest":
        """Create a new batch request

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

        payload = remove_undefined(
            {
                "processRequest": self._parse_process_request(process_request),
                "input": input,
                "output": output,
                "instance_type": instance_type,
                "description": description,
                **kwargs,
            }
        )

        request_info = self.client.get_json_dict(self._get_processing_url(), post_values=payload, use_session=True)

        return BatchRequest.from_dict(request_info)

    def _parse_process_request(self, process_request: Union[SentinelHubRequest, JsonDict]) -> dict:
        if isinstance(process_request, SentinelHubRequest):
            request_dict = process_request.download_list[0].post_values
        else:
            request_dict = process_request

        if not isinstance(request_dict, dict):
            raise ValueError(
                "Parameter sentinelhub_request should be an instance of SentinelHubRequest or a "
                "dictionary with a request payload"
            )

        return request_dict

    def geopackage_input(): ...

    @staticmethod
    def tiling_grid_input(
        grid_id: int, resolution: float, buffer: Optional[Tuple[int, int]] = None, **kwargs: Any
    ) -> JsonDict:
        """A helper method to build a dictionary with tiling grid parameters

        :param grid_id: An ID of a tiling grid
        :param resolution: A grid resolution
        :param buffer: Optionally, a buffer around each tile can be defined. It can be defined with a tuple of integers
            `(buffer_x, buffer_y)`, which specifies a number of buffer pixels in horizontal and vertical directions.
        :param kwargs: Any other arguments to be added to a dictionary of parameters
        :return: A dictionary with parameters
        """
        payload = {"id": grid_id, "resolution": resolution, **kwargs}
        if buffer:
            payload = {**payload, "bufferX": buffer[0], "bufferY": buffer[1]}
        return payload

    @staticmethod
    def raster_output(
        *,
        default_tile_path: Optional[str] = None,
        overwrite: Optional[bool] = None,
        skip_existing: Optional[bool] = None,
        cog_output: Optional[bool] = None,
        cog_parameters: Optional[Dict[str, Any]] = None,
        create_collection: Optional[bool] = None,
        collection_id: Optional[str] = None,
        responses: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """A helper method to build a dictionary with tiling grid parameters

        :param default_tile_path: A path or a template on an s3 bucket where to store results. More info at Batch API
            documentation
        :param overwrite: A flag specifying if a request should overwrite existing outputs without failing
        :param skip_existing: A flag specifying if existing outputs should be overwritten
        :param cog_output: A flag specifying if outputs should be written in COGs (cloud-optimized GeoTIFFs )or
            normal GeoTIFFs
        :param cog_parameters: A dictionary specifying COG creation parameters
        :param create_collection: If True the results will be written in COGs and a batch collection will be created
        :param collection_id: If True results will be added to an existing collection
        :param responses: Specification of path template for individual outputs/responses
        :param kwargs: Any other arguments to be added to a dictionary of parameters
        :return: A dictionary of output parameters
        """
        return remove_undefined(
            {
                "defaultTilePath": default_tile_path,
                "overwrite": overwrite,
                "skipExisting": skip_existing,
                "cogOutput": cog_output,
                "cogParameters": cog_parameters,
                "createCollection": create_collection,
                "collectionId": collection_id,
                "responses": responses,
                **kwargs,
            }
        )

    def zarr_output(): ...

    def iter_requests(
        self, user_id: Optional[str] = None, search: Optional[str] = None, sort: Optional[str] = None, **kwargs: Any
    ) -> Iterator["BatchRequest"]:
        """Iterate existing batch requests

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
            yield BatchRequest.from_dict(request_info)

    def get_request(self, batch_request: BatchRequestType) -> "BatchRequest":
        """Collects information about a single batch request."""
        request_id = self._parse_request_id(batch_request)
        request_info = self.client.get_json_dict(url=self._get_processing_url(request_id), use_session=True)
        return BatchRequest.from_dict(request_info)

    def update_request(self, batch_request: BatchRequestType, description: str) -> Json:
        """Update certain batch job request parameters. Can only update requests that are not currently being processed.

        :param batch_request: Batch request ID, a dictionary containing an "ID" field, or a BatchRequest object.
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

        :param batch_request: Batch request ID, a dictionary containing an "ID" field, or a BatchRequest object.
        """
        return self._call_job(batch_request, "analyse")

    def start_job(self, batch_request: BatchRequestType) -> Json:
        """Starts running a batch job

        :param batch_request: Batch request ID, a dictionary containing an "ID" field, or a BatchRequest object.
        """
        return self._call_job(batch_request, "start")

    def stop_job(self, batch_request: BatchRequestType) -> Json:
        """Stops a batch job

        :param batch_request: Batch request ID, a dictionary containing an "ID" field, or a BatchRequest object.
        """
        return self._call_job(batch_request, "cancel")

    def iter_tiling_grids(self, **kwargs: Any) -> SentinelHubFeatureIterator:
        """An iterator over tiling grids

        :param kwargs: Any other request query parameters
        :return: An iterator over tiling grid definitions
        """
        return SentinelHubFeatureIterator(
            client=self.client,
            url=f"{self._get_service_url()}/tilinggrids",
            params=remove_undefined(kwargs),
            exception_message="Failed to obtain information about available tiling grids",
        )

    def get_tiling_grid(self, grid_id: int) -> JsonDict:
        """Provides a single tiling grid

        :param grid_id: An ID of a requested tiling grid
        :return: A tiling grid definition
        """
        url = f"{self._get_service_url()}/tilinggrids/{grid_id}"
        return self.client.get_json_dict(url=url, use_session=True)


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass(repr=False)
class BatchRequest(BaseBatchRequest):  # pylint: disable=abstract-method
    """A dataclass object that holds information about a batch request"""

    # dataclass_json doesn't handle parameter inheritance correctly
    # pylint: disable=duplicate-code

    request_id: str = field(metadata=dataclass_config(field_name="id"))
    process_request: dict
    tile_count: int
    status: BatchRequestStatus = field(metadata=enum_config(BatchRequestStatus))
    user_id: Optional[str] = None
    created: Optional[dt.datetime] = field(metadata=datetime_config, default=None)
    tiling_grid: dict = field(default_factory=dict)
    output: dict = field(default_factory=dict)
    bucket_name: Optional[str] = None
    description: Optional[str] = None
    value_estimate: Optional[float] = None
    tile_width_px: Optional[int] = None
    tile_height_px: Optional[int] = None
    user_action: Optional[BatchUserAction] = field(metadata=enum_config(BatchUserAction), default=None)
    user_action_updated: Optional[str] = field(metadata=datetime_config, default=None)
    error: Optional[str] = None
    other_data: CatchAll = field(default_factory=dict)

    _REPR_PARAM_NAMES = (
        "request_id",
        "description",
        "bucket_name",
        "created",
        "status",
        "user_action",
        "value_estimate",
        "tile_count",
    )

    @property
    def evalscript(self) -> str:
        """Provides an evalscript used by a batch request

        :return: An evalscript
        """
        return self.process_request["evalscript"]

    @property
    def bbox(self) -> Optional[BBox]:
        """Provides a bounding box used by a batch request

        :return: An area bounding box together with CRS
        :raises: ValueError
        """
        bbox, _, crs = self._parse_bounds_payload()
        return None if bbox is None else BBox(bbox, crs)  # type: ignore[arg-type]

    @property
    def geometry(self) -> Optional[Geometry]:
        """Provides a geometry used by a batch request

        :return: An area geometry together with CRS
        :raises: ValueError
        """
        _, geometry, crs = self._parse_bounds_payload()
        return None if geometry is None else Geometry(geometry, crs)

    def _parse_bounds_payload(self) -> Tuple[Optional[List[float]], Optional[list], CRS]:
        """Parses bbox, geometry and crs from batch request payload. If bbox or geometry don't exist it returns None
        instead.
        """
        bounds_definition = self.process_request["input"]["bounds"]
        crs = CRS(bounds_definition["properties"]["crs"].rsplit("/", 1)[-1])

        return bounds_definition.get("bbox"), bounds_definition.get("geometry"), crs

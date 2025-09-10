"""
Module implementing an interface with
`Sentinel Hub Batch Processing API <https://docs.sentinel-hub.com/api/latest/api/batch/>`__.
"""

# ruff: noqa: FA100
# do not use `from __future__ import annotations`, it clashes with `dataclass_json`
import datetime as dt
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

from dataclasses_json import CatchAll, LetterCase, Undefined, dataclass_json
from dataclasses_json import config as dataclass_config

from ...geometry import CRS, BBox, Geometry
from ..base import BaseCollection
from ..utils import datetime_config, enum_config
from .base import BaseBatchRequest, BatchRequestStatus, BatchUserAction

LOGGER = logging.getLogger(__name__)

BatchRequestType = Union[str, dict, "BatchRequest"]
BatchCollectionType = Union[str, dict, "BatchCollection"]


class BatchTileStatus(Enum):
    """An enum class with all possible batch tile statuses"""

    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


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


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass
class BatchCollectionBatchData:
    """Dataclass to hold batch collection batchData part of the payload"""

    tiling_grid_id: Optional[int] = None
    other_data: CatchAll = field(default_factory=dict)


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass
class BatchCollectionAdditionalData:
    """Dataclass to hold batch collection additionalData part of the payload"""

    bands: Optional[Dict[str, Any]] = None
    other_data: CatchAll = field(default_factory=dict)


class BatchCollection(BaseCollection):
    """Dataclass for batch collections"""

    batch_data: Optional[BatchCollectionBatchData] = None
    additional_data: Optional[BatchCollectionAdditionalData] = None

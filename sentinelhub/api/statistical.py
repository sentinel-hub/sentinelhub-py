"""
Implementation of
`Sentinel Hub Statistical API interface <https://docs.sentinel-hub.com/api/latest/api/statistical/>`__.
"""
from typing import Any, List, Optional, Tuple, Union

from ..constants import MimeType
from ..download.sentinelhub_statistical_client import SentinelHubStatisticalDownloadClient
from ..geometry import BBox, Geometry
from ..time_utils import parse_time_interval, serialize_time
from ..type_utils import JsonDict, RawTimeIntervalType
from .base_request import InputDataDict, SentinelHubBaseApiRequest
from .utils import _update_other_args


class SentinelHubStatistical(SentinelHubBaseApiRequest):
    """Sentinel Hub Statistical API interface

    For more information check
    `Statistical API documentation <https://docs.sentinel-hub.com/api/latest/api/statistical/>`__.
    """

    _SERVICE_ENDPOINT = "statistics"

    def __init__(
        self,
        aggregation: JsonDict,
        input_data: List[Union[JsonDict, InputDataDict]],
        bbox: Optional[BBox] = None,
        geometry: Optional[Geometry] = None,
        calculations: Optional[JsonDict] = None,
        **kwargs: Any
    ):
        """
        For details of certain parameters check the
        `Statistical API reference <https://docs.sentinel-hub.com/api/latest/reference/#tag/statistical>`_.

        :param aggregation: Aggregation part of the payload, which can be generated with `aggregation` method
        :param input_data: A list of input dictionary objects as described in the API reference. It can be generated
            with `input_data` method
        :param bbox: A bounding box of the request
        :param geometry: A geometry of the request
        :param calculations: Calculations part of the payload.
        :param calculations: dict
        :param data_folder: Location of the directory where the downloaded data could be saved.
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        self.payload = self.body(
            request_bounds=self.bounds(bbox=bbox, geometry=geometry),
            request_data=input_data,
            aggregation=aggregation,
            calculations=calculations,
        )

        super().__init__(SentinelHubStatisticalDownloadClient, **kwargs)

    @property
    def mime_type(self) -> MimeType:
        return MimeType.JSON

    @staticmethod
    def body(
        request_bounds: JsonDict,
        request_data: List[JsonDict],
        aggregation: JsonDict,
        calculations: Optional[JsonDict],
        other_args: Optional[JsonDict] = None,
    ) -> JsonDict:
        """Generate the Process API request body

        :param request_bounds: A dictionary as generated by `bounds` helper method.
        :param request_data: A list of dictionaries as generated by `input_data` helper method.
        :param aggregation: A dictionary as generated by `aggregation` helper method.
        :param calculations: A dictionary defining calculations part of the payload
        :param other_args: Additional dictionary of arguments. If provided, the resulting dictionary will get updated
            by it.
        :returns: Request payload dictionary
        """
        # Some parts of the payload have to be defined:
        for input_data_payload in request_data:
            if "dataFilter" not in input_data_payload:
                input_data_payload["dataFilter"] = {}

        if calculations is None:
            calculations = {"default": {}}

        request_body = {
            "input": {"bounds": request_bounds, "data": request_data},
            "aggregation": aggregation,
            "calculations": calculations,
        }

        if other_args:
            _update_other_args(request_body, other_args)

        return request_body

    @staticmethod
    def aggregation(
        evalscript: str,
        time_interval: RawTimeIntervalType,
        aggregation_interval: str,
        size: Optional[Tuple[int, int]] = None,
        resolution: Optional[Tuple[float, float]] = None,
        other_args: Optional[JsonDict] = None,
    ) -> JsonDict:
        """Generate the `aggregation` part of the Statistical API request body

        :param evalscript: An `evalscript <https://docs.sentinel-hub.com/api/latest/#/Evalscript/>`_.
        :param time_interval: An interval with start and end date of the form YYYY-MM-DDThh:mm:ss or YYYY-MM-DD or
            a datetime object
        :param aggregation_interval: How data from given time interval is aggregated together
        :param size: A width and height of an image from which data will be aggregated
        :param resolution: A resolution in x and y dimensions of an image from which data will be aggregated.
            Resolution has to be defined in the same units as request bbox or geometry
        :param other_args: Additional dictionary of arguments. If provided, the resulting dictionary will get updated
            by it.
        :returns: Aggregation payload dictionary
        """
        start_time, end_time = serialize_time(parse_time_interval(time_interval, allow_undefined=True), use_tz=True)

        payload: JsonDict = {
            "evalscript": evalscript,
            "timeRange": {"from": start_time, "to": end_time},
            "aggregationInterval": {"of": aggregation_interval},
        }

        if size:
            payload["width"], payload["height"] = size
        if resolution:
            payload["resx"], payload["resy"] = resolution

        if other_args:
            _update_other_args(payload, other_args)

        return payload

"""
Implementation of base Sentinel Hub interfaces
"""
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Optional

from ..base import DataRequest
from ..constants import MimeType, MosaickingOrder, RequestType, ResamplingType
from ..data_collections import DataCollection, OrbitDirection
from ..download import DownloadRequest
from ..geometry import BBox, Geometry
from ..time_utils import RawTimeIntervalType, parse_time_interval, serialize_time
from .utils import _update_other_args


class SentinelHubBaseApiRequest(DataRequest, metaclass=ABCMeta):
    """A base class for Sentinel Hub interfaces"""

    _SERVICE_ENDPOINT = ""
    payload: Dict[str, Any] = {}

    @property
    @abstractmethod
    def mime_type(self) -> MimeType:
        """The mime type of the request."""

    def create_request(self) -> None:
        """Prepares a download request"""
        headers = {"content-type": MimeType.JSON.get_string(), "accept": self.mime_type.get_string()}
        base_url = self._get_base_url()
        self.download_list = [
            DownloadRequest(
                request_type=RequestType.POST,
                url=f"{base_url}/api/v1/{self._SERVICE_ENDPOINT}",
                post_values=self.payload,
                data_folder=self.data_folder,
                save_response=bool(self.data_folder),
                data_type=self.mime_type,
                headers=headers,
                use_session=True,
            )
        ]

    @staticmethod
    def input_data(
        data_collection: DataCollection,
        *,
        identifier: Optional[str] = None,
        time_interval: Optional[RawTimeIntervalType] = None,
        maxcc: Optional[float] = None,
        mosaicking_order: Optional[MosaickingOrder] = None,
        upsampling: Optional[ResamplingType] = None,
        downsampling: Optional[ResamplingType] = None,
        other_args: Optional[Dict[str, Any]] = None,
    ) -> "InputDataDict":
        """Generate the `input data` part of the request body

        :param data_collection: One of supported Process API data collections.
        :param identifier: A collection identifier that can be referred to in the evalscript. Parameter is referenced
            as `"id"` in service documentation. To learn more check
            `data fusion documentation <https://docs.sentinel-hub.com/api/latest/data/data-fusion>`__.
        :param time_interval: A time interval with start and end date of the form YYYY-MM-DDThh:mm:ss or YYYY-MM-DD or
            a datetime object
        :param maxcc: Maximum accepted cloud coverage of an image. Float between 0.0 and 1.0. Default is 1.0.
        :param mosaicking_order: Mosaicking order, which has to be either 'mostRecent', 'leastRecent' or 'leastCC'.
        :param upsampling: A type of upsampling to apply on data
        :param downsampling: A type of downsampling to apply on data
        :param other_args: Additional dictionary of arguments. If provided, the resulting dictionary will get updated
            by it.
        :return: A dictionary-like object that also contains additional attributes
        """
        input_data_dict: Dict[str, Any] = {
            "type": data_collection.api_id,
        }
        if identifier:
            input_data_dict["id"] = identifier

        data_filters = _get_data_filters(data_collection, time_interval, maxcc, mosaicking_order)
        if data_filters:
            input_data_dict["dataFilter"] = data_filters

        processing_params = _get_processing_params(upsampling, downsampling)
        if processing_params:
            input_data_dict["processing"] = processing_params

        if other_args:
            _update_other_args(input_data_dict, other_args)

        return InputDataDict(input_data_dict, service_url=data_collection.service_url)

    @staticmethod
    def bounds(
        bbox: Optional[BBox] = None, geometry: Optional[Geometry] = None, other_args: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Generate a `bound` part of the API request

        :param bbox: Bounding box describing the area of interest.
        :param geometry: Geometry describing the area of interest.
        :param other_args: Additional dictionary of arguments. If provided, the resulting dictionary will get updated
            by it.
        """
        if bbox is None and geometry is None:
            raise ValueError("'bbox' and/or 'geometry' have to be provided.")

        if bbox and not isinstance(bbox, BBox):
            raise ValueError("'bbox' should be an instance of sentinelhub.BBox")

        if geometry and not isinstance(geometry, Geometry):
            raise ValueError("'geometry' should be an instance of sentinelhub.Geometry")

        if bbox and geometry and bbox.crs != geometry.crs:
            raise ValueError("bbox and geometry should be in the same CRS")

        crs = bbox.crs if bbox else geometry.crs  # type: ignore[union-attr]

        request_bounds: Dict[str, Any] = {"properties": {"crs": crs.opengis_string}}

        if bbox:
            request_bounds["bbox"] = list(bbox)

        if geometry:
            request_bounds["geometry"] = geometry.get_geojson(with_crs=False)

        if other_args:
            _update_other_args(request_bounds, other_args)

        return request_bounds

    def _get_base_url(self) -> str:
        """It decides which base URL to use. Restrictions from data collection definitions overrule the
        settings from config object. In case different collections have different restrictions then
        `SHConfig.sh_base_url` breaks the tie in case it matches one of the data collection URLs.
        """
        data_collection_urls = tuple(
            {
                input_data_dict.service_url.rstrip("/")
                for input_data_dict in self.payload["input"]["data"]
                if isinstance(input_data_dict, InputDataDict) and input_data_dict.service_url is not None
            }
        )
        config_base_url = self.config.sh_base_url.rstrip("/")

        if not data_collection_urls:
            return config_base_url

        if len(data_collection_urls) == 1:
            return data_collection_urls[0]

        if config_base_url in data_collection_urls:
            return config_base_url

        raise ValueError(
            f"Given data collections are restricted to different services: {data_collection_urls}\n"
            "Configuration parameter sh_base_url cannot break the tie because it is set to a different"
            f"service: {config_base_url}"
        )


class InputDataDict(dict):
    """An input data dictionary which also holds additional attributes"""

    def __init__(self, input_data_dict: Dict[str, Any], *, service_url: Optional[str] = None):
        """
        :param input_data_dict: A normal dictionary with input parameters
        :param service_url: A service URL defined by a data collection
        """
        super().__init__(input_data_dict)
        self.service_url = service_url

    def __repr__(self) -> str:
        """Modified dictionary representation that also shows additional attributes"""
        normal_dict_repr = super().__repr__()
        return f"{self.__class__.__name__}({normal_dict_repr}, service_url={self.service_url})"


def _get_data_filters(
    data_collection: DataCollection,
    time_interval: Optional[RawTimeIntervalType],
    maxcc: Optional[float],
    mosaicking_order: Optional[MosaickingOrder],
) -> Dict[str, Any]:
    """Builds a dictionary of data filters for Process API"""
    data_filter: Dict[str, Any] = {}

    if time_interval:
        start_time, end_time = serialize_time(parse_time_interval(time_interval, allow_undefined=True), use_tz=True)
        data_filter["timeRange"] = {"from": start_time, "to": end_time}

    if maxcc is not None:
        if maxcc < 0 or maxcc > 1:
            raise ValueError("maxcc should be a float on an interval [0, 1]")

        data_filter["maxCloudCoverage"] = int(maxcc * 100)

    if mosaicking_order:
        data_filter["mosaickingOrder"] = MosaickingOrder(mosaicking_order).value

    return {**data_filter, **_get_data_collection_filters(data_collection)}


def _get_data_collection_filters(data_collection: DataCollection) -> Dict[str, Any]:
    """Builds a dictionary of filters for Process API from a data collection definition"""
    filters: Dict[str, Any] = {}

    if data_collection.swath_mode:
        filters["acquisitionMode"] = data_collection.swath_mode.upper()

    if data_collection.polarization:
        filters["polarization"] = data_collection.polarization.upper()

    if data_collection.resolution:
        filters["resolution"] = data_collection.resolution.upper()

    if data_collection.orbit_direction and data_collection.orbit_direction.upper() != OrbitDirection.BOTH:
        filters["orbitDirection"] = data_collection.orbit_direction.upper()

    if data_collection.timeliness:
        filters["timeliness"] = data_collection.timeliness

    if data_collection.dem_instance:
        filters["demInstance"] = data_collection.dem_instance

    return filters


def _get_processing_params(
    upsampling: Optional[ResamplingType], downsampling: Optional[ResamplingType]
) -> Dict[str, Any]:
    """Builds a dictionary of processing parameters for Process API"""
    processing_params: Dict[str, Any] = {}

    if upsampling:
        processing_params["upsampling"] = ResamplingType(upsampling).value

    if downsampling:
        processing_params["downsampling"] = ResamplingType(downsampling).value

    return processing_params

"""
Module for working with Sentinel Hub FIS service
"""
import datetime
import warnings
from typing import Any, List, Optional, Union

from ..constants import HistogramType, MimeType, RequestType, ServiceType
from ..download import DownloadRequest
from ..exceptions import SHDeprecationWarning
from ..geometry import BBox, Geometry
from ..time_utils import RawTimeIntervalType, RawTimeType
from .ogc import OgcImageService, OgcRequest
from .wfs import WebFeatureService


class FisRequest(OgcRequest):
    """``Deprecated - use Statistical API instead!``

    The class for interacting with Feature Info Service.

    For more info check `FIS documentation <https://www.sentinel-hub.com/develop/api/ogc/fis-request/>`__.
    """

    DEPRECATION_MESSAGE = (
        "Fis service is being deprecated in favour of SentinelHubStatistical. Although no immediate action is needed as"
        " FIS is still supported, consider switching to Statistical API because it provides additional functionalities."
    )

    def __init__(
        self,
        layer: str,
        time: Union[RawTimeType, RawTimeIntervalType],
        geometry_list: List[Union[Geometry, BBox]],
        *,
        resolution: str = "10m",
        bins: Optional[str] = None,
        histogram_type: Optional[HistogramType] = None,
        **kwargs: Any,
    ):
        """
        :param layer: An ID of a layer configured in Sentinel Hub Dashboard. It has to be configured for the same
            instance ID which will be used for this request. The satellite collection of the layer in Dashboard
            must also match the one given by `data_collection` parameter.
        :param time: time or time range for which to return the results, in ISO8601 format
            (year-month-date, for example: ``2016-01-01``, or year-month-dateThours:minutes:seconds format,
            i.e. ``2016-01-01T16:31:21``). Examples: ``'2016-01-01'``, or ``('2016-01-01', ' 2016-01-31')``
            (datetime.datetime, datetime.datetime)
        :param geometry_list: A WKT representation of a geometry describing the region of interest.
            Note that WCS 1.1.1 standard is used here, so for EPSG:4326 coordinates should be in latitude/longitude
            order.
        :param resolution: Specifies the spatial resolution, in meters per pixel, of the image from which the statistics
            are to be estimated. When using CRS=EPSG:4326 one has to add the "m" suffix to
            enforce resolution in meters per pixel (e.g. RESOLUTION=10m).
        :param bins: The number of bins (a positive integer) in the histogram. If this parameter is absent no histogram
            is computed.
        :param histogram_type: type of histogram
        :param data_collection: A collection of requested satellite data. It has to be the same as defined in Sentinel
            Hub Dashboard for the given layer. Default is Sentinel-2 L1C.
        :param maxcc: maximum accepted cloud coverage of an image. Float between 0.0 and 1.0. Default is ``1.0``.
        :param custom_url_params: Dictionary of CustomUrlParameters and their values supported by Sentinel Hub's WMS
            and WCS services. All available parameters are described in
            `documentation <https://www.sentinel-hub.com/develop/api/ogc/custom-parameters/>`__. Note: in
            case of constants.CustomUrlParam.EVALSCRIPT the dictionary value must be a string
            of Javascript code that is not encoded into base64.
        :param data_folder: location of the directory where the fetched data will be saved.
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        self.geometry_list = geometry_list
        self.resolution = resolution
        self.bins = bins
        self.histogram_type = HistogramType(histogram_type) if histogram_type else None

        super().__init__(
            bbox=None, layer=layer, time=time, service_type=ServiceType.FIS, **kwargs  # type: ignore[arg-type]
        )

    def create_request(self) -> None:  # type: ignore[override]
        """Set download requests

        Create a list of DownloadRequests for all Sentinel-2 acquisitions within request's time interval and
        acceptable cloud coverage.
        """

        warnings.warn(self.DEPRECATION_MESSAGE, category=SHDeprecationWarning, stacklevel=2)
        fis_service = _FisService(config=self.config)
        self.download_list = fis_service.get_request(self)

    def get_dates(self) -> List[Optional[datetime.datetime]]:
        """This method is not supported for FIS request"""
        raise NotImplementedError

    def get_tiles(self) -> Optional[WebFeatureService]:
        """This method is not supported for FIS request"""
        raise NotImplementedError


class _FisService(OgcImageService):
    """Sentinel Hub OGC services class for providing FIS data

    Intermediate layer between FIS requests and the Sentinel Hub FIS services.
    """

    def get_request(self, request: FisRequest) -> List[DownloadRequest]:  # type: ignore[override]
        """Get download requests

        Create a list of DownloadRequests for all Sentinel-2 acquisitions within request's time interval and
        acceptable cloud coverage.

        :param request: OGC-type request with specified bounding box, time interval, and cloud coverage for specific
            product.
        """

        return [self._create_request(request=request, geometry=geometry) for geometry in request.geometry_list]

    def _create_request(self, request: FisRequest, geometry: Union[BBox, Geometry]) -> DownloadRequest:
        url = self.get_base_url(request)

        post_data = {**self._get_common_url_parameters(request), **self._get_fis_parameters(request, geometry)}
        post_data = {k.lower(): v for k, v in post_data.items()}  # lowercase required on SH service

        return DownloadRequest(
            url=f"{url}/{self.config.instance_id}",
            post_values=post_data,
            data_type=MimeType.JSON,
            headers={"Content-Type": MimeType.JSON.get_string()},
            request_type=RequestType.POST,
        )

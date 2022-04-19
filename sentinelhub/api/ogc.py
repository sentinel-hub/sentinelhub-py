"""
Module for working with Sentinel Hub OGC services
`Sentinel Hub OGC services <https://www.sentinel-hub.com/develop/api/ogc/standard-parameters/>`__.
"""
import datetime
import logging
from base64 import b64encode
from urllib.parse import urlencode

from ..base import DataRequest
from ..config import SHConfig
from ..constants import CRS, CustomUrlParam, MimeType, ResamplingType, ServiceType, SHConstants
from ..download import DownloadRequest, SentinelHubDownloadClient
from ..geo_utils import get_image_dimension
from ..geometry import BBox, Geometry
from ..time_utils import filter_times, parse_time_interval, serialize_time
from .wfs import WebFeatureService

LOGGER = logging.getLogger(__name__)


class OgcRequest(DataRequest):
    """The base class for OGC-type requests (WMS and WCS) where all common parameters are defined"""

    def __init__(
        self,
        layer,
        bbox,
        *,
        data_collection,
        time="latest",
        service_type=None,
        size_x=None,
        size_y=None,
        maxcc=1.0,
        image_format=MimeType.PNG,
        custom_url_params=None,
        time_difference=datetime.timedelta(seconds=-1),
        **kwargs,
    ):
        """
        :param layer: An ID of a layer configured in Sentinel Hub Dashboard. It has to be configured for the same
            instance ID which will be used for this request. The satellite collection of the layer in Dashboard
            must also match the one given by `data_collection` parameter
        :type layer: str
        :param bbox: Bounding box of the requested image. Coordinates must be in the specified coordinate reference
            system.
        :type bbox: geometry.BBox
        :param data_collection: A collection of requested satellite data. It has to be the same as defined in
            Sentinel Hub Dashboard for the given layer.
        :type data_collection: DataCollection
        :param time: time or time range for which to return the results, in ISO8601 format
            (year-month-date, for example: ``2016-01-01``, or year-month-dateThours:minutes:seconds format,
            i.e. ``2016-01-01T16:31:21``). When a single time is specified the request will return data for that
            specific date, if it exists. If a time range is specified the result is a list of all scenes between the
            specified dates conforming to the cloud coverage criteria. Most recent acquisition being first in the list.
            For the latest acquisition use ``latest``. Examples: ``latest``, ``'2016-01-01'``, or
            ``('2016-01-01', ' 2016-01-31')``
        :type time: str or (str, str) or datetime.date or (datetime.date, datetime.date) or datetime.datetime or
            (datetime.datetime, datetime.datetime)
        :param service_type: type of OGC service (WMS or WCS)
        :type service_type: constants.ServiceType
        :param size_x: number of pixels in x or resolution in x (i.e. ``512`` or ``10m``)
        :type size_x: int or str
        :param size_y: number of pixels in x or resolution in y (i.e. ``512`` or ``10m``)
        :type size_y: int or str
        :param maxcc: maximum accepted cloud coverage of an image. Float between 0.0 and 1.0. Default is ``1.0``.
        :type maxcc: float
        :param image_format: format of the returned image by the Sentinel Hub's WMS getMap service. Default is PNG, but
                            in some cases 32-bit TIFF is required, i.e. if requesting unprocessed raw bands.
                            Default is ``constants.MimeType.PNG``.
        :type image_format: constants.MimeType
        :param custom_url_params: A dictionary of CustomUrlParameters and their values supported by Sentinel Hub's WMS
            and WCS services. All available parameters are described in
            `documentation <https://www.sentinel-hub.com/develop/api/ogc/custom-parameters/>`__. Note: in case of
            `CustomUrlParam.EVALSCRIPT` the dictionary value must be a string of Javascript code that is not
            encoded into base64.
        :type custom_url_params: Dict[CustomUrlParameter, object]
        :param time_difference: The time difference below which dates are deemed equal. That is, if for the given set
            of OGC parameters the images are available at datetimes `d1<=d2<=...<=dn` then only those with
            `dk-dj>time_difference` will be considered. The default time difference is negative (`-1s`), meaning
            that all dates are considered by default.
        :type time_difference: datetime.timedelta
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.layer = layer
        self.bbox = bbox
        self.time = time
        self.data_collection = data_collection
        self.maxcc = maxcc
        self.image_format = MimeType(image_format)
        self.service_type = service_type
        self.size_x = size_x
        self.size_y = size_y
        self.custom_url_params = custom_url_params
        self.time_difference = time_difference

        if self.custom_url_params is not None:
            self._check_custom_url_parameters()

        self.wfs_iterator = None

        super().__init__(SentinelHubDownloadClient, **kwargs)

    def _check_custom_url_parameters(self):
        """Checks if custom url parameters are valid parameters.

        Throws ValueError if the provided parameter is not a valid parameter.
        """
        for param in self.custom_url_params:
            if param not in CustomUrlParam:
                raise ValueError(f"Parameter {param} is not a valid custom url parameter. Please check and fix.")

        if self.service_type is ServiceType.FIS and CustomUrlParam.GEOMETRY in self.custom_url_params:
            raise ValueError(f"{CustomUrlParam.GEOMETRY} should not be a custom url parameter of a FIS request")

    def create_request(self, reset_wfs_iterator=False):
        """Set download requests

        Create a list of DownloadRequests for all Sentinel-2 acquisitions within request's time interval and
        acceptable cloud coverage.

        :param reset_wfs_iterator: When re-running the method this flag is used to reset/keep existing ``wfs_iterator``
            (i.e. instance of ``WebFeatureService`` class). If the iterator is not reset you don't have to repeat a
            service call but tiles and dates will stay the same.
        :type reset_wfs_iterator: bool
        """
        if reset_wfs_iterator:
            self.wfs_iterator = None

        ogc_service = OgcImageService(config=self.config)
        self.download_list = ogc_service.get_request(self)
        self.wfs_iterator = ogc_service.get_wfs_iterator()

    def get_dates(self):
        """Get list of dates

        List of all available Sentinel-2 acquisitions for given bbox with max cloud coverage and the specified
        time interval. When a single time is specified the request will return that specific date, if it exists.
        If a time range is specified the result is a list of all scenes between the specified dates conforming to
        the cloud coverage criteria. Most recent acquisition being first in the list.

        :return: list of all available Sentinel-2 acquisition times within request's time interval and
            acceptable cloud coverage.
        :rtype: list(datetime.datetime) or [None]
        """
        return OgcImageService(config=self.config).get_dates(self)

    def get_tiles(self):
        """Returns iterator over info about all satellite tiles used for the OgcRequest

        :return: Iterator of dictionaries containing info about all satellite tiles used in the request. In case of
            `DataCollection.DEM` it returns None.
        :rtype: Iterator[dict] or None
        """
        return self.wfs_iterator


class WmsRequest(OgcRequest):
    """Web Map Service request class

    Creates an instance of Sentinel Hub WMS (Web Map Service) GetMap request,
    which provides access to Sentinel-2's unprocessed bands (B01, B02, ..., B08, B8A, ..., B12)
    or processed products such as true color imagery, NDVI, etc. The only difference is that in
    the case of WMS request the user specifies the desired image size instead of its resolution.

    It is required to specify at least one of `width` and `height` parameters. If only one of them is specified the
    other one will be calculated to best fit the bounding box ratio. If both of them are specified they will be used
    no matter the bounding box ratio.

    For more info check `WMS documentation <https://www.sentinel-hub.com/develop/api/ogc/standard-parameters/wms/>`__.
    """

    def __init__(self, *, width=None, height=None, **kwargs):
        """
        :param width: width (number of columns) of the returned image (array)
        :type width: int or None
        :param height: height (number of rows) of the returned image (array)
        :type height: int or None
        :param layer: An ID of a layer configured in Sentinel Hub Dashboard. It has to be configured for the same
            instance ID which will be used for this request. The satellite collection of the layer in Dashboard
            must also match the one given by `data_collection` parameter
        :type layer: str
        :param bbox: Bounding box of the requested image. Coordinates must be in the specified coordinate reference
            system.
        :type bbox: geometry.BBox
        :param time: time or time range for which to return the results, in ISO8601 format
            (year-month-date, for example: ``2016-01-01``, or year-month-dateThours:minutes:seconds format,
            i.e. ``2016-01-01T16:31:21``). When a single time is specified the request will return data for that
            specific date, if it exists. If a time range is specified the result is a list of all scenes between the
            specified dates conforming to the cloud coverage criteria. Most recent acquisition being first in the list.
            For the latest acquisition use ``latest``. Examples: ``latest``, ``'2016-01-01'``, or
            ``('2016-01-01', ' 2016-01-31')``
        :type time: str or (str, str) or datetime.date or (datetime.date, datetime.date) or datetime.datetime or
            (datetime.datetime, datetime.datetime)
        :param data_collection: A collection of requested satellite data. It has to be the same as defined in
            Sentinel Hub Dashboard for the given layer. Default is Sentinel-2 L1C.
        :type data_collection: DataCollection
        :param size_x: number of pixels in x or resolution in x (i.e. ``512`` or ``10m``)
        :type size_x: int or str
        :param size_y: number of pixels in x or resolution in y (i.e. ``512`` or ``10m``)
        :type size_y: int or str
        :param maxcc: maximum accepted cloud coverage of an image. Float between 0.0 and 1.0. Default is ``1.0``.
        :type maxcc: float
        :param image_format: format of the returned image by the Sentinel Hub's WMS getMap service. Default is PNG, but
                            in some cases 32-bit TIFF is required, i.e. if requesting unprocessed raw bands.
                            Default is ``constants.MimeType.PNG``.
        :type image_format: constants.MimeType
        :param custom_url_params: A dictionary of CustomUrlParameters and their values supported by Sentinel Hub's WMS
            and WCS services. All available parameters are described in
            `documentation <https://www.sentinel-hub.com/develop/api/ogc/custom-parameters/>`__. Note: in case of
            `CustomUrlParam.EVALSCRIPT` the dictionary value must be a string of Javascript code that is not
            encoded into base64.
        :type custom_url_params: Dict[CustomUrlParameter, object]
        :param time_difference: The time difference below which dates are deemed equal. That is, if for the given set
            of OGC parameters the images are available at datetimes `d1<=d2<=...<=dn` then only those with
            `dk-dj>time_difference` will be considered. The default time difference is negative (`-1s`), meaning
            that all dates are considered by default.
        :type time_difference: datetime.timedelta
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        super().__init__(service_type=ServiceType.WMS, size_x=width, size_y=height, **kwargs)


class WcsRequest(OgcRequest):
    """Web Coverage Service request class

    Creates an instance of Sentinel Hub WCS (Web Coverage Service) GetCoverage request,
    which provides access to Sentinel-2's unprocessed bands (B01, B02, ..., B08, B8A, ..., B12)
    or processed products such as true color imagery, NDVI, etc., as the WMS service. The
    only difference is that in the case of WCS request the user specifies the desired
    resolution of the image instead of its size.

    For more info check `WCS documentation <https://www.sentinel-hub.com/develop/api/ogc/standard-parameters/wcs/>`__.
    """

    def __init__(self, *, resx="10m", resy="10m", **kwargs):
        """
        :param resx: resolution in x (resolution of a column) given in meters in the format (examples ``10m``,
            ``20m``, ...). Default is ``10m``, which is the best native resolution of some Sentinel-2 bands.
        :type resx: str
        :param resy: resolution in y (resolution of a row) given in meters in the format
            (examples ``10m``, ``20m``, ...). Default is ``10m``, which is the best native resolution of some
            Sentinel-2 bands.
        :type resy: str
        :param layer: An ID of a layer configured in Sentinel Hub Dashboard. It has to be configured for the same
            instance ID which will be used for this request. The satellite collection of the layer in Dashboard
            must also match the one given by `data_collection` parameter
        :type layer: str
        :param bbox: Bounding box of the requested image. Coordinates must be in the specified coordinate reference
            system.
        :type bbox: geometry.BBox
        :param time: time or time range for which to return the results, in ISO8601 format
            (year-month-date, for example: ``2016-01-01``, or year-month-dateThours:minutes:seconds format,
            i.e. ``2016-01-01T16:31:21``). When a single time is specified the request will return data for that
            specific date, if it exists. If a time range is specified the result is a list of all scenes between the
            specified dates conforming to the cloud coverage criteria. Most recent acquisition being first in the list.
            For the latest acquisition use ``latest``. Examples: ``latest``, ``'2016-01-01'``, or
            ``('2016-01-01', ' 2016-01-31')``
        :type time: str or (str, str) or datetime.date or (datetime.date, datetime.date) or datetime.datetime or
            (datetime.datetime, datetime.datetime)
        :param data_collection: A collection of requested satellite data. It has to be the same as defined in Sentinel
            Hub Dashboard for the given layer. Default is Sentinel-2 L1C.
        :type data_collection: DataCollection
        :param size_x: number of pixels in x or resolution in x (i.e. ``512`` or ``10m``)
        :type size_x: int or str
        :param size_y: number of pixels in x or resolution in y (i.e. ``512`` or ``10m``)
        :type size_y: int or str
        :param maxcc: maximum accepted cloud coverage of an image. Float between 0.0 and 1.0. Default is ``1.0``.
        :type maxcc: float
        :param image_format: format of the returned image by the Sentinel Hub's WMS getMap service. Default is PNG, but
                            in some cases 32-bit TIFF is required, i.e. if requesting unprocessed raw bands.
                            Default is ``constants.MimeType.PNG``.
        :type image_format: constants.MimeType
        :param custom_url_params: A dictionary of CustomUrlParameters and their values supported by Sentinel Hub's WMS
            and WCS services. All available parameters are described in
            `documentation <https://www.sentinel-hub.com/develop/api/ogc/custom-parameters/>`__. Note: in case of
            `CustomUrlParam.EVALSCRIPT` the dictionary value must be a string of Javascript code that is not
            encoded into base64.
        :type custom_url_params: Dict[CustomUrlParameter, object]
        :param time_difference: The time difference below which dates are deemed equal. That is, if for the given set
            of OGC parameters the images are available at datetimes `d1<=d2<=...<=dn` then only those with
            `dk-dj>time_difference` will be considered. The default time difference is negative (`-1s`), meaning
            that all dates are considered by default.
        :type time_difference: datetime.timedelta
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        super().__init__(service_type=ServiceType.WCS, size_x=resx, size_y=resy, **kwargs)


class OgcImageService:
    """Sentinel Hub OGC services class for providing image data

    Intermediate layer between QGC-type requests (WmsRequest and WcsRequest) and the Sentinel Hub OGC (WMS and WCS)
    services.
    """

    def __init__(self, config=None):
        """
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.config = config or SHConfig()
        self.config.raise_for_missing_instance_id()

        self._base_url = self.config.get_sh_ogc_url()
        self.wfs_iterator = None

    def get_request(self, request):
        """Get download requests

        Create a list of DownloadRequests for all Sentinel-2 acquisitions within request's time interval and
        acceptable cloud coverage.

        :param request: QGC-type request with specified bounding box, time interval, and cloud coverage for specific
                        product.
        :type request: OgcRequest or GeopediaRequest
        :return: list of DownloadRequests
        """
        size_x, size_y = self.get_image_dimensions(request)
        return [
            DownloadRequest(
                url=self.get_url(request=request, date=date, size_x=size_x, size_y=size_y),
                data_type=request.image_format,
                headers=SHConstants.HEADERS,
            )
            for date in self.get_dates(request)
        ]

    def get_url(self, request, *, date=None, size_x=None, size_y=None, geometry=None):
        """Returns url to Sentinel Hub's OGC service for the product specified by the OgcRequest and date.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :param date: acquisition date or None
        :type date: datetime.datetime or None
        :param size_x: horizontal image dimension
        :type size_x: int or str
        :param size_y: vertical image dimension
        :type size_y: int or str
        :type geometry: list of BBox or Geometry
        :return:  dictionary with parameters
        :return: url to Sentinel Hub's OGC service for this product.
        :rtype: str
        """
        url = self.get_base_url(request)
        authority = request.theme if hasattr(request, "theme") else self.config.instance_id

        params = self._get_common_url_parameters(request)
        if request.service_type in (ServiceType.WMS, ServiceType.WCS):
            params = {**params, **self._get_wms_wcs_url_parameters(request, date)}
        if request.service_type is ServiceType.WMS:
            params = {**params, **self._get_wms_url_parameters(request, size_x, size_y)}
        elif request.service_type is ServiceType.WCS:
            params = {**params, **self._get_wcs_url_parameters(request, size_x, size_y)}
        elif request.service_type is ServiceType.FIS:
            params = {**params, **self._get_fis_parameters(request, geometry)}

        return f"{url}/{authority}?{urlencode(params)}"

    def get_base_url(self, request):
        """Creates base url string.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :return: base string for url to Sentinel Hub's OGC service for this product.
        :rtype: str
        """
        url = f"{self._base_url}/{request.service_type.value}"

        if hasattr(request, "data_collection") and request.data_collection.service_url:
            url = url.replace(self.config.sh_base_url, request.data_collection.service_url)

        return url

    @staticmethod
    def _get_common_url_parameters(request):
        """Returns parameters common dictionary for WMS, WCS and FIS request.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :return:  dictionary with parameters
        :rtype: dict
        """
        params = {"SERVICE": request.service_type.value, "WARNINGS": False}

        if hasattr(request, "maxcc"):
            params["MAXCC"] = 100.0 * request.maxcc

        if hasattr(request, "custom_url_params") and request.custom_url_params is not None:
            custom_params = request.custom_url_params

            if CustomUrlParam.EVALSCRIPT in custom_params:
                evalscript = custom_params[CustomUrlParam.EVALSCRIPT]
                custom_params[CustomUrlParam.EVALSCRIPT] = b64encode(evalscript.encode()).decode()

            if CustomUrlParam.GEOMETRY in custom_params:
                geometry = custom_params[CustomUrlParam.GEOMETRY]
                crs = request.bbox.crs

                if isinstance(geometry, Geometry):
                    if geometry.crs is not crs:
                        raise ValueError("Geometry object in custom_url_params should have the same CRS as given BBox")
                else:
                    geometry = Geometry(geometry, crs)

                if geometry.crs is CRS.WGS84:
                    geometry = geometry.reverse()

                custom_params[CustomUrlParam.GEOMETRY] = geometry.wkt

            for resampling in (CustomUrlParam.DOWNSAMPLING, CustomUrlParam.UPSAMPLING):
                if resampling in custom_params:
                    custom_params[resampling] = ResamplingType(custom_params[resampling]).value

            params.update({k.value: str(v) for k, v in custom_params.items()})

        return params

    @staticmethod
    def _get_wms_wcs_url_parameters(request, date):
        """Returns parameters common dictionary for WMS and WCS request.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :param date: acquisition date or None
        :type date: datetime.datetime or None
        :return:  dictionary with parameters
        :rtype: dict
        """
        params = {
            "BBOX": str(request.bbox.reverse()) if request.bbox.crs is CRS.WGS84 else str(request.bbox),
            "FORMAT": MimeType.get_string(request.image_format),
            "CRS": CRS.ogc_string(request.bbox.crs),
        }

        if date is not None:
            start_date = (
                date if request.time_difference < datetime.timedelta(seconds=0) else date - request.time_difference
            )
            end_date = (
                date if request.time_difference < datetime.timedelta(seconds=0) else date + request.time_difference
            )

            start_date, end_date = serialize_time((start_date, end_date), use_tz=True)
            params["TIME"] = f"{start_date}/{end_date}"

        return params

    @staticmethod
    def _get_wms_url_parameters(request, size_x, size_y):
        """Returns parameters dictionary for WMS request.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :param size_x: horizontal image dimension
        :type size_x: int or str
        :param size_y: vertical image dimension
        :type size_y: int or str
        :return:  dictionary with parameters
        :rtype: dict
        """
        return {"WIDTH": size_x, "HEIGHT": size_y, "LAYERS": request.layer, "REQUEST": "GetMap", "VERSION": "1.3.0"}

    @staticmethod
    def _get_wcs_url_parameters(request, size_x, size_y):
        """Returns parameters dictionary for WCS request.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :param size_x: horizontal image dimension
        :type size_x: int or str
        :param size_y: vertical image dimension
        :type size_y: int or str
        :return:  dictionary with parameters
        :rtype: dict
        """
        return {"RESX": size_x, "RESY": size_y, "COVERAGE": request.layer, "REQUEST": "GetCoverage", "VERSION": "1.1.2"}

    @staticmethod
    def _get_fis_parameters(request, geometry):
        """Returns parameters dictionary for FIS request.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :param geometry: list of bounding boxes or geometries
        :type geometry: list of BBox or Geometry
        :return:  dictionary with parameters
        :rtype: dict
        """
        start_time, end_time = serialize_time(parse_time_interval(request.time), use_tz=True)

        params = {
            "CRS": CRS.ogc_string(geometry.crs),
            "LAYER": request.layer,
            "RESOLUTION": request.resolution,
            "TIME": f"{start_time}/{end_time}",
        }

        if not isinstance(geometry, (BBox, Geometry)):
            raise ValueError(
                f"Each geometry must be an instance of sentinelhub.{BBox.__name__} or "
                f"sentinelhub.{Geometry.__name__} but {geometry} found"
            )
        if geometry.crs is CRS.WGS84:
            geometry = geometry.reverse()
        if isinstance(geometry, Geometry):
            params["GEOMETRY"] = geometry.wkt
        else:
            params["BBOX"] = str(geometry)

        if request.bins:
            params["BINS"] = request.bins

        if request.histogram_type:
            params["TYPE"] = request.histogram_type.value

        return params

    def get_dates(self, request):
        """Get available Sentinel-2 acquisitions at least time_difference apart

        List of all available Sentinel-2 acquisitions for given bbox with max cloud coverage and the specified
        time interval. When a single time is specified the request will return that specific date, if it exists.
        If a time range is specified the result is a list of all scenes between the specified dates conforming to
        the cloud coverage criteria. Most recent acquisition being first in the list.

        When a time_difference threshold is set to a positive value, the function filters out all datetimes which
        are within the time difference. The oldest datetime is preserved, all others all deleted.

        :param request: OGC-type request
        :type request: WmsRequest or WcsRequest
        :return: List of dates of existing acquisitions for the given request
        :rtype: list(datetime.datetime) or [None]
        """
        if request.data_collection.is_timeless:
            return [None]

        if request.wfs_iterator is None:
            self.wfs_iterator = WebFeatureService(
                request.bbox,
                request.time,
                data_collection=request.data_collection,
                maxcc=request.maxcc,
                config=self.config,
            )
        else:
            self.wfs_iterator = request.wfs_iterator

        dates = self.wfs_iterator.get_dates()
        dates = filter_times(dates, request.time_difference)

        LOGGER.debug("Initializing requests for dates: %s", dates)
        return dates

    @staticmethod
    def get_image_dimensions(request):
        """Verifies or calculates image dimensions.

        :param request: OGC-type request
        :type request: WmsRequest or WcsRequest
        :return: horizontal and vertical dimensions of requested image
        :rtype: (int or str, int or str)
        """
        if request.service_type is ServiceType.WCS or (
            isinstance(request.size_x, int) and isinstance(request.size_y, int)
        ):
            return request.size_x, request.size_y
        if not isinstance(request.size_x, int) and not isinstance(request.size_y, int):
            raise ValueError("At least one of parameters 'width' and 'height' must have an integer value")
        missing_dimension = get_image_dimension(request.bbox, width=request.size_x, height=request.size_y)
        if request.size_x is None:
            return missing_dimension, request.size_y
        if request.size_y is None:
            return request.size_x, missing_dimension
        raise ValueError("Parameters 'width' and 'height' must be integers or None")

    def get_wfs_iterator(self):
        """Returns iterator over info about all satellite tiles used for the request

        :return: Iterator of dictionaries containing info about all satellite tiles used in the request. In case of
            `DataCollection.DEM` it returns `None`.
        :rtype: Iterator[dict] or None
        """
        return self.wfs_iterator

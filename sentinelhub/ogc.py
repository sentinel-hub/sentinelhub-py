"""
Module for working with Sentinel Hub OGC services
"""
import logging
import datetime
from base64 import b64encode
from urllib.parse import urlencode

import shapely.geometry

from .constants import ServiceType, MimeType, CRS, SHConstants, CustomUrlParam
from .config import SHConfig
from .data_collections import DataCollection, handle_deprecated_data_source
from .geo_utils import get_image_dimension
from .geometry import BBox, Geometry
from .download import DownloadRequest, SentinelHubDownloadClient
from .sh_utils import FeatureIterator
from .time_utils import parse_time, parse_time_interval, serialize_time, filter_times

LOGGER = logging.getLogger(__name__)


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
        """ Get download requests

        Create a list of DownloadRequests for all Sentinel-2 acquisitions within request's time interval and
        acceptable cloud coverage.

        :param request: QGC-type request with specified bounding box, time interval, and cloud coverage for specific
                        product.
        :type request: OgcRequest or GeopediaRequest
        :return: list of DownloadRequests
        """
        size_x, size_y = self.get_image_dimensions(request)
        return [DownloadRequest(url=self.get_url(request=request, date=date, size_x=size_x, size_y=size_y),
                                data_type=request.image_format, headers=SHConstants.HEADERS)
                for date in self.get_dates(request)]

    def get_url(self, request, *, date=None, size_x=None, size_y=None, geometry=None):
        """ Returns url to Sentinel Hub's OGC service for the product specified by the OgcRequest and date.

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
        authority = request.theme if hasattr(request, 'theme') else self.config.instance_id

        params = self._get_common_url_parameters(request)
        if request.service_type in (ServiceType.WMS, ServiceType.WCS):
            params = {**params, **self._get_wms_wcs_url_parameters(request, date)}
        if request.service_type is ServiceType.WMS:
            params = {**params, **self._get_wms_url_parameters(request, size_x, size_y)}
        elif request.service_type is ServiceType.WCS:
            params = {**params, **self._get_wcs_url_parameters(request, size_x, size_y)}
        elif request.service_type is ServiceType.FIS:
            params = {**params, **self._get_fis_parameters(request, geometry)}

        return f'{url}/{authority}?{urlencode(params)}'

    def get_base_url(self, request):
        """ Creates base url string.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :return: base string for url to Sentinel Hub's OGC service for this product.
        :rtype: str
        """
        url = f'{self._base_url}/{request.service_type.value}'

        if hasattr(request, 'data_collection') and request.data_collection.service_url:
            url = url.replace(self.config.sh_base_url, request.data_collection.service_url)

        return url

    @staticmethod
    def _get_common_url_parameters(request):
        """ Returns parameters common dictionary for WMS, WCS and FIS request.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :return:  dictionary with parameters
        :rtype: dict
        """
        params = {
            'SERVICE': request.service_type.value,
            'WARNINGS': False
        }

        if hasattr(request, 'maxcc'):
            params['MAXCC'] = 100.0 * request.maxcc

        if hasattr(request, 'custom_url_params') and request.custom_url_params is not None:
            params = {**params,
                      **{k.value: str(v) for k, v in request.custom_url_params.items()}}

            if CustomUrlParam.EVALSCRIPT.value in params:
                evalscript = params[CustomUrlParam.EVALSCRIPT.value]
                params[CustomUrlParam.EVALSCRIPT.value] = b64encode(evalscript.encode()).decode()

            if CustomUrlParam.GEOMETRY.value in params:
                geometry = params[CustomUrlParam.GEOMETRY.value]
                crs = request.bbox.crs

                if isinstance(geometry, Geometry):
                    if geometry.crs is not crs:
                        raise ValueError('Geometry object in custom_url_params should have the same CRS as given BBox')
                else:
                    geometry = Geometry(geometry, crs)

                if geometry.crs is CRS.WGS84:
                    geometry = geometry.reverse()

                params[CustomUrlParam.GEOMETRY.value] = geometry.wkt

        return params

    @staticmethod
    def _get_wms_wcs_url_parameters(request, date):
        """ Returns parameters common dictionary for WMS and WCS request.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :param date: acquisition date or None
        :type date: datetime.datetime or None
        :return:  dictionary with parameters
        :rtype: dict
        """
        params = {
            'BBOX': str(request.bbox.reverse()) if request.bbox.crs is CRS.WGS84 else str(request.bbox),
            'FORMAT': MimeType.get_string(request.image_format),
            'CRS': CRS.ogc_string(request.bbox.crs),
        }

        if date is not None:
            start_date = date if request.time_difference < datetime.timedelta(
                seconds=0) else date - request.time_difference
            end_date = date if request.time_difference < datetime.timedelta(
                seconds=0) else date + request.time_difference

            start_date, end_date = serialize_time((start_date, end_date), use_tz=True)
            params['TIME'] = f'{start_date}/{end_date}'

        return params

    @staticmethod
    def _get_wms_url_parameters(request, size_x, size_y):
        """ Returns parameters dictionary for WMS request.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :param size_x: horizontal image dimension
        :type size_x: int or str
        :param size_y: vertical image dimension
        :type size_y: int or str
        :return:  dictionary with parameters
        :rtype: dict
        """
        return {
            'WIDTH': size_x,
            'HEIGHT': size_y,
            'LAYERS': request.layer,
            'REQUEST': 'GetMap',
            'VERSION': '1.3.0'
        }

    @staticmethod
    def _get_wcs_url_parameters(request, size_x, size_y):
        """ Returns parameters dictionary for WCS request.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :param size_x: horizontal image dimension
        :type size_x: int or str
        :param size_y: vertical image dimension
        :type size_y: int or str
        :return:  dictionary with parameters
        :rtype: dict
        """
        return {
            'RESX': size_x,
            'RESY': size_y,
            'COVERAGE': request.layer,
            'REQUEST': 'GetCoverage',
            'VERSION': '1.1.2'
        }

    @staticmethod
    def _get_fis_parameters(request, geometry):
        """ Returns parameters dictionary for FIS request.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :param geometry: list of bounding boxes or geometries
        :type geometry: list of BBox or Geometry
        :return:  dictionary with parameters
        :rtype: dict
        """
        start_time, end_time = serialize_time(parse_time_interval(request.time), use_tz=True)

        params = {
            'CRS': CRS.ogc_string(geometry.crs),
            'LAYER': request.layer,
            'RESOLUTION': request.resolution,
            'TIME': f'{start_time}/{end_time}'
        }

        if not isinstance(geometry, (BBox, Geometry)):
            raise ValueError(f'Each geometry must be an instance of sentinelhub.{BBox.__name__} or '
                             f'sentinelhub.{Geometry.__name__} but {geometry} found')
        if geometry.crs is CRS.WGS84:
            geometry = geometry.reverse()
        if isinstance(geometry, Geometry):
            params['GEOMETRY'] = geometry.wkt
        else:
            params['BBOX'] = str(geometry)

        if request.bins:
            params['BINS'] = request.bins

        if request.histogram_type:
            params['TYPE'] = request.histogram_type.value

        return params

    def get_dates(self, request):
        """ Get available Sentinel-2 acquisitions at least time_difference apart

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
            self.wfs_iterator = WebFeatureService(request.bbox, request.time, data_collection=request.data_collection,
                                                  maxcc=request.maxcc, config=self.config)
        else:
            self.wfs_iterator = request.wfs_iterator

        dates = self.wfs_iterator.get_dates()
        dates = filter_times(dates, request.time_difference)

        LOGGER.debug('Initializing requests for dates: %s', dates)
        return dates

    @staticmethod
    def get_image_dimensions(request):
        """ Verifies or calculates image dimensions.

        :param request: OGC-type request
        :type request: WmsRequest or WcsRequest
        :return: horizontal and vertical dimensions of requested image
        :rtype: (int or str, int or str)
        """
        if request.service_type is ServiceType.WCS or (isinstance(request.size_x, int) and
                                                       isinstance(request.size_y, int)):
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
        """ Returns iterator over info about all satellite tiles used for the request

        :return: Iterator of dictionaries containing info about all satellite tiles used in the request. In case of
                 DataCollection.DEM it returns None.
        :rtype: Iterator[dict] or None
        """
        return self.wfs_iterator


class WebFeatureService(FeatureIterator):
    """ Class for interaction with Sentinel Hub WFS service

    The class is an iterator over info data of all available satellite tiles for requested parameters. It collects data
    from Sentinel Hub service only during the first iteration. During next iterations it returns already obtained data.
    The data is in the order returned by Sentinel Hub WFS service.
    """
    def __init__(self, bbox, time_interval, *, data_collection=None, maxcc=1.0, data_source=None, config=None):
        """
        :param bbox: Bounding box of the requested image. Coordinates must be in the specified coordinate reference
            system.
        :type bbox: geometry.BBox
        :param time_interval: interval with start and end date of the form YYYY-MM-DDThh:mm:ss or YYYY-MM-DD
        :type time_interval: (str, str)
        :param data_collection: A collection of requested satellite data
        :type data_collection: DataCollection
        :param maxcc: Maximum accepted cloud coverage of an image. Float between 0.0 and 1.0. Default is 1.0.
        :type maxcc: float
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        :param data_source: A deprecated alternative to data_collection
        :type data_source: DataCollection
        """
        self.config = config or SHConfig()
        self.config.raise_for_missing_instance_id()

        self.bbox = bbox

        self.latest_time_only = time_interval == SHConstants.LATEST
        if self.latest_time_only:
            self.time_interval = datetime.datetime(year=1985, month=1, day=1), datetime.datetime.now()
        else:
            self.time_interval = parse_time_interval(time_interval)

        self.data_collection = DataCollection(handle_deprecated_data_source(data_collection, data_source,
                                                                            default=DataCollection.SENTINEL2_L1C))
        self.maxcc = maxcc
        self.max_features_per_request = 1 if self.latest_time_only else self.config.max_wfs_records_per_query

        client = SentinelHubDownloadClient(config=self.config)
        url = self._build_service_url()
        params = self._build_request_params()

        super().__init__(client, url, params)
        self.next = 0

    def _build_service_url(self):
        """ Creates an URL to WFS service
        """
        base_url = self.config.get_sh_ogc_url()

        if self.data_collection.service_url:
            base_url = base_url.replace(self.config.sh_base_url, self.data_collection.service_url)

        return f'{base_url}/{ServiceType.WFS.value}/{self.config.instance_id}'

    def _build_request_params(self):
        """ Builds URL parameters for WFS service
        """
        start_time, end_time = serialize_time(self.time_interval, use_tz=True)
        return {
            'SERVICE': ServiceType.WFS.value,
            'WARNINGS': False,
            'REQUEST': 'GetFeature',
            'TYPENAMES': self.data_collection.wfs_id,
            'BBOX': str(self.bbox.reverse()) if self.bbox.crs is CRS.WGS84 else str(self.bbox),
            'OUTPUTFORMAT': MimeType.JSON.get_string(),
            'SRSNAME': self.bbox.crs.ogc_string(),
            'TIME': f'{start_time}/{end_time}',
            'MAXCC': 100.0 * self.maxcc,
            'MAXFEATURES': self.max_features_per_request
        }

    def _fetch_features(self):
        """ Collects data from WFS service
        """
        params = {
            **self.params,
            'FEATURE_OFFSET': self.next
        }
        url = f'{self.url}?{urlencode(params)}'

        LOGGER.debug("URL=%s", url)
        response = self.client.get_json(url)

        new_features = response['features']

        if len(new_features) < self.max_features_per_request or self.latest_time_only:
            self.finished = True
        else:
            self.next += self.max_features_per_request

        is_sentinel1 = self.data_collection.is_sentinel1
        new_features = [feature_info for feature_info in new_features
                        if not is_sentinel1 or self._sentinel1_product_check(feature_info)]
        return new_features

    def get_dates(self):
        """ Returns a list of acquisition times from tile info data

        :return: List of acquisition times in the order returned by WFS service.
        :rtype: list(datetime.datetime)
        """
        tile_dates = []

        for tile_info in self:
            if not tile_info['properties']['date']:  # could be True for custom (BYOC) data collections
                tile_dates.append(None)
            else:
                date_str = tile_info['properties']['date']
                time_str = tile_info['properties']['time']
                tile_dates.append(parse_time(f'{date_str}T{time_str}'))

        return tile_dates

    def get_geometries(self):
        """ Returns a list of geometries from tile info data

        :return: List of multipolygon geometries in the order returned by WFS service.
        :rtype: list(shapely.geometry.MultiPolygon)
        """
        return [shapely.geometry.shape(tile_info['geometry']) for tile_info in self]

    def get_tiles(self):
        """ Returns list of tiles with tile name, date and AWS index

        :return: List of tiles in form of (tile_name, date, aws_index)
        :rtype: list((str, str, int))
        """
        return [self._parse_tile_url(tile_info['properties']['path']) for tile_info in self]

    @staticmethod
    def _parse_tile_url(tile_url):
        """ Extracts tile name, data and AWS index from tile URL

        :param tile_url: Location of tile at AWS
        :type tile_url: str
        :return: Tuple in a form (tile_name, date, aws_index)
        :rtype: (str, str, int)
        """
        props = tile_url.rsplit('/', 7)
        return ''.join(props[1:4]), '-'.join(props[4:7]), int(props[7])

    def _sentinel1_product_check(self, tile_info):
        """ Checks if Sentinel-1 tile info match the data collection definition
        """
        product_id = tile_info['properties']['id']
        props = product_id.split('_')
        swath_mode, resolution, polarization = props[1], props[2][3], props[3][2:4]
        orbit_direction = tile_info['properties'].get('orbitDirection', '')

        if not (swath_mode in ['IW', 'EW'] and resolution in ['M', 'H'] and polarization in ['DV', 'DH', 'SV', 'SH']):
            raise ValueError(f'Unknown Sentinel-1 tile type: {product_id}')

        return (swath_mode == self.data_collection.swath_mode or self.data_collection.swath_mode is None) \
            and (polarization == self.data_collection.polarization or self.data_collection.polarization is None) \
            and (resolution == self.data_collection.resolution[0] or self.data_collection.resolution is None) \
            and self.data_collection.contains_orbit_direction(orbit_direction)

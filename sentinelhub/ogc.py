"""
Module for working with Sentinel Hub OGC services
"""

import logging
import datetime
from base64 import b64encode
from urllib.parse import urlencode

import shapely.geometry
import shapely.wkt
import shapely.ops

from .constants import ServiceType, DataSource, MimeType, CRS, SHConstants, CustomUrlParam
from .config import SHConfig
from .geo_utils import get_image_dimension
from .geometry import BBox, Geometry
from .download import DownloadRequest, get_json
from .time_utils import parse_time_interval

LOGGER = logging.getLogger(__name__)


class OgcService:
    """ The base class for Sentinel Hub OGC services
    """
    def __init__(self, base_url=None, instance_id=None):
        """
        :param base_url: base url of Sentinel Hub's OGC services. If `None`, the url specified in the configuration
                    file is taken.
        :type base_url: str or None
        :param instance_id: user's instance id granting access to Sentinel Hub's OGC services. If `None`, the instance
            ID specified in the configuration file is taken.
        :type instance_id: str or None
        """
        self.base_url = SHConfig().get_sh_ogc_url() if not base_url else base_url
        self.instance_id = SHConfig().instance_id if not instance_id else instance_id

        if not self.instance_id:
            raise ValueError('Instance ID is not set. '
                             'Set it either in request initialization or in configuration file. '
                             'Check http://sentinelhub-py.readthedocs.io/en/latest/configure.html for more info.')

    @staticmethod
    def _filter_dates(dates, time_difference):
        """ Filters out dates within time_difference, preserving only the oldest date.

        :param dates: a list of datetime objects
        :param time_difference: a ``datetime.timedelta`` representing the time difference threshold
        :return: an ordered list of datetimes `d1<=d2<=...<=dn` such that `d[i+1]-di > time_difference`
        :rtype: list(datetime.datetime)
        """

        LOGGER.debug("dates=%s", dates)

        if len(dates) <= 1:
            return dates

        sorted_dates = sorted(dates)

        separate_dates = [sorted_dates[0]]
        for curr_date in sorted_dates[1:]:
            if curr_date - separate_dates[-1] > time_difference:
                separate_dates.append(curr_date)
        return separate_dates

    @staticmethod
    def _sentinel1_product_check(product_id, data_source):
        """ Checks if Sentinel-1 product ID matches Sentinel-1 DataSource configuration

        :param product_id: Sentinel-1 product ID
        :type product_id: str
        :param data_source: One of the supported Sentinel-1 data sources
        :type data_source: constants.DataSource
        :return: `True` if data_source contains product_id and `False` otherwise
        :rtype: bool
        """
        props = product_id.split('_')
        acquisition, resolution, polarisation = props[1], props[2][3], props[3][2:4]
        if acquisition in ['IW', 'EW'] and resolution in ['M', 'H'] and polarisation in ['DV', 'DH', 'SV', 'SH']:
            return acquisition == data_source.value[2].name and polarisation == data_source.value[3].name and \
                   resolution == data_source.value[4].name[0]
        raise ValueError('Unknown Sentinel-1 tile type: {}'.format(product_id))


class OgcImageService(OgcService):
    """Sentinel Hub OGC services class for providing image data

    Intermediate layer between QGC-type requests (WmsRequest and WcsRequest) and the Sentinel Hub OGC (WMS and WCS)
    services.
    """
    def __init__(self, **kwargs):
        """
        :param base_url: base url of Sentinel Hub's OGC services. If `None`, the url specified in the configuration
                    file is taken.
        :type base_url: str or None
        :param instance_id: user's instance id granting access to Sentinel Hub's OGC services. If `None`, the instance
            ID specified in the configuration file is taken.
        :type instance_id: str or None
        """
        super().__init__(**kwargs)

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
                                filename=self.get_filename(request, date, size_x, size_y),
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
        authority = self.instance_id if hasattr(self, 'instance_id') else request.theme

        params = self._get_common_url_parameters(request)
        if request.service_type in (ServiceType.WMS, ServiceType.WCS):
            params = {**params, **self._get_wms_wcs_url_parameters(request, date)}
        if request.service_type is ServiceType.WMS:
            params = {**params, **self._get_wms_url_parameters(request, size_x, size_y)}
        elif request.service_type is ServiceType.WCS:
            params = {**params, **self._get_wcs_url_parameters(request, size_x, size_y)}
        elif request.service_type is ServiceType.FIS:
            params = {**params, **self._get_fis_parameters(request, geometry)}

        return '{}/{}?{}'.format(url, authority, urlencode(params))

    def get_base_url(self, request):
        """ Creates base url string.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :return: base string for url to Sentinel Hub's OGC service for this product.
        :rtype: str
        """
        url = '{}/{}'.format(self.base_url, request.service_type.value)
        # These 2 lines are temporal and will be removed after the use of uswest url wont be required anymore:
        if hasattr(request, 'data_source') and request.data_source.is_uswest_source():
            url = 'https://services-uswest2.sentinel-hub.com/ogc/{}'.format(request.service_type.value)

        if hasattr(request, 'data_source') and request.data_source not in DataSource.get_available_sources():
            raise ValueError("{} is not available for service at service available at {}. Try"
                             "changing 'sh_base_url' parameter in package configuration "
                             "file".format(request.data_source, SHConfig().get_sh_ogc_url()))
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
            'SERVICE': request.service_type.value
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
            params['TIME'] = '{}/{}'.format(start_date.isoformat(), end_date.isoformat())

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
        date_interval = parse_time_interval(request.time)

        params = {
            'CRS': CRS.ogc_string(geometry.crs),
            'LAYER': request.layer,
            'RESOLUTION': request.resolution,
            'TIME': '{}/{}'.format(date_interval[0], date_interval[1])
        }

        if not isinstance(geometry, (BBox, Geometry)):
            raise ValueError('Each geometry must be an instance of sentinelhub.{} or sentinelhub.{} but {} '
                             'found'.format(BBox.__name__, Geometry.__name__, geometry))
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

    @staticmethod
    def get_filename(request, date, size_x, size_y):
        """ Get filename location

        Returns the filename's location on disk where data is or is going to be stored.
        The files are stored in the folder specified by the user when initialising OGC-type
        of request. The name of the file has the following structure:

        {service_type}_{layer}_{crs}_{bbox}_{time}_{size_x}X{size_y}_*{custom_url_params}.{image_format}

        In case of `TIFF_d32f` a `'_tiff_depth32f'` is added at the end of the filename (before format suffix)
        to differentiate it from 16-bit float tiff.

        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :param date: acquisition date or None
        :type date: datetime.datetime or None
        :param size_x: horizontal image dimension
        :type size_x: int or str
        :param size_y: vertical image dimension
        :type size_y: int or str
        :return: filename for this request and date
        :rtype: str
        """
        filename = '_'.join([
            str(request.service_type.value),
            request.layer,
            str(request.bbox.crs),
            str(request.bbox).replace(',', '_'),
            '' if date is None else date.strftime("%Y-%m-%dT%H-%M-%S"),
            '{}X{}'.format(size_x, size_y)
        ])

        filename = OgcImageService.filename_add_custom_url_params(filename, request)

        return OgcImageService.finalize_filename(filename, request.image_format)

    @staticmethod
    def filename_add_custom_url_params(filename, request):
        """ Adds custom url parameters to filename string

        :param filename: Initial filename
        :type filename: str
        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest or GeopediaRequest
        :return: Filename with custom url parameters in the name
        :rtype: str
        """
        if hasattr(request, 'custom_url_params') and request.custom_url_params is not None:
            for param, value in sorted(request.custom_url_params.items(),
                                       key=lambda parameter_item: parameter_item[0].value):
                filename = '_'.join([filename, param.value, str(value)])

        return filename

    @staticmethod
    def finalize_filename(filename, file_format=None):
        """ Replaces invalid characters in filename string, adds image extension and reduces filename length

        :param filename: Incomplete filename string
        :type filename: str
        :param file_format: Format which will be used for filename extension
        :type file_format: MimeType
        :return: Final filename string
        :rtype: str
        """
        for char in [' ', '/', '\\', '|', ';', ':', '\n', '\t']:
            filename = filename.replace(char, '')

        if file_format:
            suffix = str(file_format.value)
            if file_format.is_tiff_format() and file_format is not MimeType.TIFF:
                suffix = str(MimeType.TIFF.value)
                filename = '_'.join([filename, str(file_format.value).replace(';', '_')])

            filename = '.'.join([filename[:254 - len(suffix)], suffix])

        LOGGER.debug("filename=%s", filename)

        return filename  # Even in UNIX systems filename must have at most 255 bytes

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
        if DataSource.is_timeless(request.data_source):
            return [None]

        date_interval = parse_time_interval(request.time)

        LOGGER.debug('date_interval=%s', date_interval)

        if request.wfs_iterator is None:
            self.wfs_iterator = WebFeatureService(request.bbox, date_interval, data_source=request.data_source,
                                                  maxcc=request.maxcc, base_url=self.base_url,
                                                  instance_id=self.instance_id)
        else:
            self.wfs_iterator = request.wfs_iterator

        dates = sorted(set(self.wfs_iterator.get_dates()))

        if request.time is SHConstants.LATEST:
            dates = dates[-1:]
        return OgcService._filter_dates(dates, request.time_difference)

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
                 DataSource.DEM it returns None.
        :rtype: Iterator[dict] or None
        """
        return self.wfs_iterator


class WebFeatureService(OgcService):
    """ Class for interaction with Sentinel Hub WFS service

    The class is an iterator over info data of all available satellite tiles for requested parameters. It collects data
    from Sentinel Hub service only during the first iteration. During next iterations it returns already obtained data.
    The data is in the order returned by Sentinel Hub WFS service.
    """
    def __init__(self, bbox, time_interval, *, data_source=DataSource.SENTINEL2_L1C, maxcc=1.0, **kwargs):
        """
        :param bbox: Bounding box of the requested image. Coordinates must be in the specified coordinate reference
            system.
        :type bbox: geometry.BBox
        :param time_interval: interval with start and end date of the form YYYY-MM-DDThh:mm:ss or YYYY-MM-DD
        :type time_interval: (str, str)
        :param data_source: Source of requested satellite data. Default is Sentinel-2 L1C data.
        :type data_source: constants.DataSource
        :param maxcc: Maximum accepted cloud coverage of an image. Float between 0.0 and 1.0. Default is 1.0.
        :type maxcc: float
        :param base_url: base url of Sentinel Hub's OGC services. If `None`, the url specified in the configuration
                        file is taken.
        :type base_url: str or None
        :param instance_id: user's instance id granting access to Sentinel Hub's OGC services. If `None`, the instance
            ID specified in the configuration file is taken.
        :type instance_id: str or None
        """
        super().__init__(**kwargs)

        self.bbox = bbox
        self.time_interval = parse_time_interval(time_interval)
        self.data_source = data_source
        self.maxcc = maxcc

        self.tile_list = []
        self.index = 0
        self.feature_offset = 0

    def __iter__(self):
        """ Iteration method

        :return: iterator of dictionaries containing info about product tiles
        :rtype: Iterator[dict]
        """
        self.index = 0
        return self

    def __next__(self):
        """ Next method

        :return: dictionary containing info about product tiles
        :rtype: dict
        """
        while self.index >= len(self.tile_list) and self.feature_offset is not None:
            self._fetch_features()

        if self.index < len(self.tile_list):
            self.index += 1
            return self.tile_list[self.index - 1]

        raise StopIteration

    def _fetch_features(self):
        """ Collects data from WFS service

        :return: dictionary containing info about product tiles
        :rtype: dict
        """
        if self.feature_offset is None:
            return

        main_url = '{}/{}/{}?'.format(self.base_url, ServiceType.WFS.value, self.instance_id)

        params = {'SERVICE': ServiceType.WFS.value,
                  'REQUEST': 'GetFeature',
                  'TYPENAMES': DataSource.get_wfs_typename(self.data_source),
                  'BBOX': str(self.bbox.reverse()) if self.bbox.crs is CRS.WGS84 else str(self.bbox),
                  'OUTPUTFORMAT': MimeType.get_string(MimeType.JSON),
                  'SRSNAME': CRS.ogc_string(self.bbox.crs),
                  'TIME': '{}/{}'.format(self.time_interval[0], self.time_interval[1]),
                  'MAXCC': 100.0 * self.maxcc,
                  'MAXFEATURES': SHConfig().max_wfs_records_per_query,
                  'FEATURE_OFFSET': self.feature_offset}

        url = main_url + urlencode(params)

        LOGGER.debug("URL=%s", url)
        response = get_json(url)

        is_sentinel1 = self.data_source.is_sentinel1()
        for tile_info in response["features"]:
            if is_sentinel1:
                if self._sentinel1_product_check(tile_info['properties']['id'], self.data_source) and \
                        self.data_source.contains_orbit_direction(tile_info['properties']['orbitDirection']):
                    self.tile_list.append(tile_info)
            else:
                self.tile_list.append(tile_info)

        if len(response["features"]) < SHConfig().max_wfs_records_per_query:
            self.feature_offset = None
        else:
            self.feature_offset += SHConfig().max_wfs_records_per_query

    def get_dates(self):
        """ Returns a list of acquisition times from tile info data

        :return: List of acquisition times in the order returned by WFS service.
        :rtype: list(datetime.datetime)
        """
        tile_dates = []

        for tile_info in self:
            if not tile_info['properties']['date']:  # could be True for custom (BYOC) data sources
                tile_dates.append(None)
            else:
                tile_dates.append(datetime.datetime.strptime('{}T{}'.
                                                             format(tile_info['properties']['date'],
                                                                    tile_info['properties']['time'].split('.')[0]),
                                                             '%Y-%m-%dT%H:%M:%S'))

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

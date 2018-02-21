"""
Module for working with Sentinel Hub OGC services
"""

import logging
import datetime
import base64

from urllib.parse import urlencode

from .time_utils import get_current_date, parse_time
from .download import DownloadRequest, get_json
from .constants import ServiceType, DataSource, MimeType, CRS, OgcConstants, CustomUrlParam
from .config import SGConfig
from .geo_utils import get_image_dimension

LOGGER = logging.getLogger(__name__)


class OgcService:
    """ Sentinel Hub OGC service class

    Intermediate layer between QGC-type requests (WmsRequest and WcsRequest) and the Sentinel Hub OGC (WMS and WCS)
    services.

    :param base_url: base url of Sentinel Hub's OGC services. If ``None``, the url specified in the configuration
                    file is taken.
    :type base_url: None or str
    :param instance_id: user's instance id granting access to Sentinel Hub's OGC services. If ``None``, the instance id
                        specified in the configuration file is taken.
    :type instance_id: None or str
    """
    def __init__(self, base_url=None, instance_id=None):
        self.base_url = SGConfig().ogc_base_url if base_url is None else base_url
        self.instance_id = SGConfig().instance_id if instance_id is None else instance_id
        if not self.instance_id:
            raise ValueError('Instance ID is not set. '
                             'Set it either in request initialisation or in configuration file.')

    def get_request(self, request):
        """ Get download requests

        Create a list of DownloadRequests for all Sentinel-2 acquisitions within request's time interval and
        acceptable cloud coverage.

        :param request: QGC request with specified bounding box, time interval, and cloud coverage for specific product.
        :type request: OgcRequest
        :return: list of DownloadRequests
        """
        size_x, size_y = self.get_image_dimensions(request)
        return [DownloadRequest(url=self.get_url(request, date, size_x, size_y),
                                filename=self.get_filename(request, date, size_x, size_y),
                                data_type=request.image_format, headers=OgcConstants.HEADERS)
                for date in self.get_dates(request)]

    def get_url(self, request, date, size_x, size_y):
        """ Returns url to Sentinel Hub's OGC service for the product specified by the OgcRequest and date.

        :param request: OGC request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest
        :param date: acquisition date of this product in `YYYY-MM-DDThh:mm:ss` format
        :type date: str or None
        :param size_x: horizontal image dimension
        :type size_x: int or str
        :param size_y: vertical image dimension
        :type size_y: int or str
        :return: url to Sentinel Hub's OGC service for this product.
        :rtype: str
        """
        url = self.base_url + request.service_type.value
        # These 2 lines are temporal and will be removed after the use of uswest url wont be required anymore:
        if DataSource.is_uswest_source(request.data_source):
            url = 'https://services-uswest2.sentinel-hub.com/ogc/{}'.format(request.service_type.value)

        params = {'SERVICE': request.service_type.value,
                  'BBOX': str(request.bbox),
                  'FORMAT': MimeType.get_string(request.image_format),
                  'CRS': CRS.ogc_string(request.bbox.get_crs()),
                  'MAXCC': 100.0 * request.maxcc}

        if request.service_type is ServiceType.WMS:
            params = {**params,
                      **{
                          'WIDTH': size_x,
                          'HEIGHT': size_y,
                          'LAYERS': request.layer,
                          'REQUEST': 'GetMap'
                      }}
        elif request.service_type is ServiceType.WCS:
            params = {**params,
                      **{
                          'RESX': size_x,
                          'RESY': size_y,
                          'COVERAGE': request.layer,
                          'REQUEST': 'GetCoverage'
                      }}

        if date is not None:
            start_date = date if request.time_difference < datetime.timedelta(
                seconds=0) else date - request.time_difference
            end_date = date if request.time_difference < datetime.timedelta(
                seconds=0) else date + request.time_difference
            params['TIME'] = '{}/{}'.format(start_date.isoformat(), end_date.isoformat())

        if request.custom_url_params is not None:
            params = {**params,
                      **{k.value: str(v) for k, v in request.custom_url_params.items()}}

            if CustomUrlParam.EVALSCRIPT.value in params:
                evalscript = params[CustomUrlParam.EVALSCRIPT.value]
                params[CustomUrlParam.EVALSCRIPT.value] = base64.b64encode(evalscript.encode()).decode()

        return '{}/{}?{}'.format(url, self.instance_id, urlencode(params))

    @staticmethod
    def get_filename(request, date, size_x, size_y):
        """ Get filename location

        Returns the filename's location on disk where data is or is going to be stored.
        The files are stored in the folder specified by the user when initialising OGC-type
        of request. The name of the file has the following structure:

        {service_type}_{layer}_{crs}_{bbox}_{time}_{size_x}X{size_y}_{custom_url_param}_
        {custom_url_param_val}.{image_format}

        In case of `TIFF_d32f` a `'_tiff_depth32f'` is added at the end of the filename (before format suffix)
        to differentiate it from 16-bit float tiff.

        :param request: OGC request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest
        :param date: acquisition date of this product in `YYYY-MM-DDThh:mm:ss` format
        :type date: str or None
        :param size_x: horizontal image dimension
        :type size_x: int or str
        :param size_y: vertical image dimension
        :type size_y: int or str
        :return: filename for this request and date
        :rtype: str
        """
        bbox_str = str(request.bbox).replace(',', '_')
        suffix = str(request.image_format.value)
        fmt = ''
        if request.image_format is MimeType.TIFF_d32f:
            suffix = str(MimeType.TIFF.value)
            fmt = str(request.image_format.value).replace(';', '_')

        filename = '_'.join([str(request.service_type.value),
                             request.layer,
                             str(request.bbox.get_crs()).replace(':', ''),
                             bbox_str,
                             '' if date is None else date.strftime("%Y-%m-%dT%H-%M-%S"),
                             str(size_x)+'X'+str(size_y)])

        LOGGER.debug("filename=%s", filename)

        if request.custom_url_params is not None:
            for param, value in sorted(request.custom_url_params.items(),
                                       key=lambda parameter_item: parameter_item[0].value):
                filename = '_'.join([filename, param.value, str(value).replace('/', '_')])

        if fmt:
            filename = '_'.join([filename, fmt])

        filename = '.'.join([filename, suffix])

        return filename

    @staticmethod
    def _parse_date_interval(time):
        """ Parses dates into common form

        Parses specified time into common form - tuple of start and end dates, i.e.:

        ``(2017-01-15:T00:00:00, 2017-01-16:T23:59:59)``

        The parameter can have the following values/format, which will be parsed as:

        * ``None`` -> `[default_start_date from config.json, current date]`
        * `YYYY-MM-DD` -> `[YYYY-MM-DD:T00:00:00, YYYY-MM-DD:T23:59:59]`
        * `YYYY-MM-DDThh:mm:ss` -> `[YYYY-MM-DDThh:mm:ss, YYYY-MM-DDThh:mm:ss]`
        * list or tuple of two dates (`YYYY-MM-DD`) -> `[YYYY-MM-DDT00:00:00, YYYY-MM-DDT23:59:59]`, where the first
          (second) element is start (end) date
        * list or tuple of two dates (`YYYY-MM-DDThh:mm:ss`) -> `[YYYY-MM-DDThh:mm:ss, YYYY-MM-DDThh:mm:ss]`,
          where the first (second) element is start (end) date

        :param time: time window of acceptable acquisitions. See above for all acceptable argument formats.
        :type time: ``None``, str of form `YYYY-MM-DD` or `'YYYY-MM-DDThh:mm:ss'`, list or tuple of two such strings
        :return: interval of start and end date of the form YYYY-MM-DDThh:mm:ss
        :rtype: tuple of start and end date
        """
        if time is None or time is OgcConstants.LATEST:
            date_interval = (SGConfig().default_start_date, get_current_date())
        else:
            if isinstance(time, str):
                date_interval = (parse_time(time), parse_time(time))
            elif isinstance(time, list) or isinstance(time, tuple) and len(time) == 2:
                date_interval = (parse_time(time[0]), parse_time(time[1]))
            else:
                raise TabError('time must be a string or tuple of 2 strings or list of 2 strings')
            if date_interval[0] > date_interval[1]:
                raise ValueError('First time must be smaller or equal to second time')

        if len(date_interval[0].split('T')) == 1:
            date_interval = (date_interval[0] + 'T00:00:00', date_interval[1])
        if len(date_interval[1].split('T')) == 1:
            date_interval = (date_interval[0], date_interval[1] + 'T23:59:59')

        return date_interval

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
        """
        if DataSource.is_timeless(request.data_source):
            return [None]

        date_interval = OgcService._parse_date_interval(request.time)

        LOGGER.debug('date_interval=%s', date_interval)

        dates = []
        for tile_info in self.wfs_search_iter(request, date_interval):
            date = tile_info['properties']['date']
            time = tile_info['properties']['time'].split('.')[0]
            dates.append(datetime.datetime.strptime('{}T{}'.format(date, time), '%Y-%m-%dT%H:%M:%S'))
        dates = sorted(set(dates))

        if request.time is OgcConstants.LATEST:
            dates = dates[-1:]
        return OgcService._filter_dates(dates, request.time_difference)

    @staticmethod
    def _filter_dates(dates, time_difference):
        """
        Filters out dates within time_difference, preserving only the oldest date.

        :param dates: a list of datetime objects
        :param time_difference: a ``datetime.timedelta`` representing the time difference threshold
        :return: an ordered list of datetimes `d1<=d2<=...<=dn` such that `d[i+1]-di > time_difference`
        :rtype: list[datetime.datetime]
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
    def get_image_dimensions(request):
        """
        Verifies or calculates image dimensions.

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

    def wfs_search_iter(self, request, date_interval):
        """Iterator method that uses Sentinel Hub WFS service to provide info about all available product for the given
        OGC-type request and date interval

        :param request: OGC-type request
        :type request: WmsRequest or WcsRequest
        :param date_interval: interval of start and end date of the form YYYY-MM-DDThh:mm:ss
        :type date_interval: (str, str)
        :return: dictionaries containing info about product tiles
        :rtype: Iterator[dict]
        """
        main_url = '{}{}/{}?'.format(self.base_url, ServiceType.WFS.value, self.instance_id)

        params = {'SERVICE': ServiceType.WFS.value,
                  'REQUEST': 'GetFeature',
                  'TYPENAMES': DataSource.get_wfs_typename(request.data_source),
                  'BBOX': str(request.bbox),
                  'OUTPUTFORMAT': MimeType.get_string(MimeType.JSON),
                  'SRSNAME': CRS.ogc_string(request.bbox.get_crs()),
                  'TIME': '{}/{}'.format(date_interval[0], date_interval[1]),
                  'MAXCC': 100.0 * request.maxcc,
                  'MAXFEATURES': SGConfig().max_wfs_records_per_query}

        is_sentinel1 = DataSource.is_sentinel1(request.data_source)
        feature_offset = 0
        while True:
            params['FEATURE_OFFSET'] = feature_offset

            url = main_url + urlencode(params)
            LOGGER.debug("URL=%s", url)

            response = get_json(url)
            for tile_info in response["features"]:
                if not is_sentinel1 or self._sentinel1_product_check(tile_info['properties']['id'],
                                                                     request.data_source):
                    yield tile_info

            if len(response["features"]) < SGConfig().max_wfs_records_per_query:
                break
            feature_offset += SGConfig().max_wfs_records_per_query

    @staticmethod
    def _sentinel1_product_check(product_id, data_source):
        """Checks if Sentinel-1 product ID matches Sentinel-1 DataSource configuration

        :param product_id: Sentinel-1 product ID
        :type product_id: str
        :param data_source: One of the supported Sentinel-1 data sources
        :type data_source: constants.DataSource
        :return: True if data_source contains product_id and False otherwise
        :rtype: bool
        """
        props = product_id.split('_')
        acquisition, resolution, polarisation = props[1], props[2][3], props[3][2:4]
        if acquisition in ['IW', 'EW'] and resolution in ['M', 'H'] and polarisation in ['DV', 'DH', 'SV', 'SH']:
            return acquisition == data_source.value[2].name and polarisation == data_source.value[3].name and \
                   resolution == data_source.value[4].name[0]
        raise ValueError('Unknown Sentinel-1 tile type: {}'.format(product_id))

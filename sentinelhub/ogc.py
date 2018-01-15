"""
Module for working with Sentinel Hub OGC services
"""

import logging
import datetime

from urllib.parse import urlencode

from .time_utils import get_current_date, parse_time
from .opensearch import get_area_dates
from .download import DownloadRequest
from .constants import CRS, DataSource, MimeType, OgcConstants
from .config import SGConfig

LOGGER = logging.getLogger(__name__)


class OgcService:
    """ Sentinel Hub OGC service class

    Intermediate layer between QGC-type requests (WmsRequest and WcsRequest) and the Sentinel Hub OGC (WMS and WCS)
    services.

    :param base_url: base url of Sentinel Hub's OGC services. If `None`, the url specified in the configuration
                    file is taken.
    :type base_url: None or str
    :param instance_id: user's instance id granting access to Sentinel Hub's OGC services. If `None`, the instance id
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
        return [DownloadRequest(url=self.get_url(request, date), filename=self.get_filename(request, date),
                                data_type=request.image_format) for date in self.get_dates(request)]

    def get_url(self, request, date):
        """ Returns url to Sentinel Hub's OGC service for the product specified by the OgcRequest and date.

        :param request: OGC request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest
        :param date: acquisition date of this product
        :type date: datetime
        :return: url to Sentinel Hub's OGC service for this product.
        """

        LOGGER.debug('MimeType: %s', request.image_format)

        url = self.base_url+str(request.source.value)

        params = {'SERVICE': str(request.source.value),
                  'BBOX': str(request.bbox),
                  'FORMAT': MimeType.get_string(request.image_format),
                  'CRS': CRS.ogc_string(request.bbox.get_crs()),
                  'MAXCC': 100.0 * request.maxcc}

        if request.source is DataSource.WMS:
            params = {**params,
                      **{
                          'WIDTH': request.size_x,
                          'HEIGHT': request.size_y,
                          'LAYERS': request.layer,
                          'REQUEST': 'GetMap'
                      }}
        elif request.source is DataSource.WCS:
            params = {**params,
                      **{
                          'RESX': request.size_x,
                          'RESY': request.size_y,
                          'COVERAGE': request.layer,
                          'REQUEST': 'GetCoverage'
                      }}

        if date is not None:
            start_date = date if request.time_difference < datetime.timedelta(
                seconds=0) else date - request.time_difference
            end_date = date if request.time_difference < datetime.timedelta(
                seconds=0) else date + request.time_difference
            params = {**params,
                      **{'TIME': '{0}/{1}'.format(start_date.isoformat(), end_date.isoformat())}}

        if request.custom_url_params is not None:
            params = {**params,
                      **{k.value: str(v) for k, v in request.custom_url_params.items()}}

        return '{0}/{1}?{2}'.format(url, self.instance_id, urlencode(params))

    @staticmethod
    def get_filename(request, date):
        """ Get filename location

        Returns the filename's location on disk where data is or is going to be stored.
        The files are stored in the folder specified by the user when initialising OGC-type
        of request. The name of the file has the following structure:

        {source}_{layer}_{crs}_{bbox}_{time}_{size_x}X{size_y}_{custom_url_param}_{custom_url_param_val}.{image_format}

        In case of TIFF_d32f a '_tiff_depth32f' is added at the end of the filename (before format suffix)
        to differentiate it from 16-bit float tiff.

        :param request: OGC request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest
        :param date: acquisition date of this product in `YYYY-MM-DDThh:mm:ss` format
        :type date: str
        :return: filename for this request and date
        """
        bbox_str = str(request.bbox).replace(',', '_')
        suffix = str(request.image_format.value)
        fmt = ''
        if request.image_format is MimeType.TIFF_d32f:
            suffix = str(MimeType.TIFF.value)
            fmt = str(request.image_format.value).replace(';', '_')

        filename = '_'.join([str(request.source.value),
                             request.layer,
                             str(request.bbox.get_crs()).replace(':', ''),
                             bbox_str,
                             date.strftime("%Y-%m-%dT%H-%M-%S"),
                             str(request.size_x)+'X'+str(request.size_y)])

        LOGGER.debug("filename=%s", filename)

        if request.custom_url_params is not None:
            for param, value in request.custom_url_params.items():
                filename = '_'.join([filename, param.value, str(value).replace('/', '_')])

        if fmt:
            filename = '_'.join([filename, fmt])

        filename = '.'.join([filename, suffix])

        return filename

    @staticmethod
    def _parse_date_interval(time):
        """ Parses dates into common form

        Parses specified time into common form - tuple of start and end dates, i.e.:

        (2017-01-15:T00:00:00, 2017-01-16:T23:59:59)

        The parameter can have the following values/format, which will be parsed as:

        * None -> [s2_start_date from config.json, current date]
        * YYYY-MM-DD -> [YYYY-MM-DD:T00:00:00, YYYY-MM-DD:T23:59:59]
        * YYYY-MM-DDThh:mm:ss -> [YYYY-MM-DDThh:mm:ss, YYYY-MM-DDThh:mm:ss]
        * list or tuple of two dates (YYYY-MM-DD) -> [YYYY-MM-DDT00:00:00, YYYY-MM-DDT23:59:59], where the first
          (second) element is start (end) date
        * list or tuple of two dates (YYYY-MM-DDThh:mm:ss) -> [YYYY-MM-DDThh:mm:ss, YYYY-MM-DDThh:mm:ss],
          where the first (second) element is start (end) date

        :param time: time window of acceptable acquisitions. See above for all acceptable argument formats.
        :type time: None, str of form `YYYY-MM-DD` or 'YYYY-MM-DDThh:mm:ss', list or tuple of two such strings
        :return: interval of start and end date of the form YYYY-MM-DDThh:mm:ss
        :rtype: tuple of start and end date
        """
        if time is None or time is OgcConstants.LATEST:
            date_interval = (SGConfig().s2_start_date, get_current_date())
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

    @staticmethod
    def get_dates(request):
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
        LOGGER.debug('CRS=%s', request.bbox.get_crs())

        date_interval = OgcService._parse_date_interval(request.time)

        LOGGER.debug('date_interval=%s', date_interval)

        if request.time is OgcConstants.LATEST:
            return OgcService._filter_dates(get_area_dates(request.bbox, date_interval, maxcc=request.maxcc)[-1:],
                                            request.time_difference)
        return OgcService._filter_dates(get_area_dates(request.bbox, date_interval, maxcc=request.maxcc),
                                        request.time_difference)

    @staticmethod
    def _filter_dates(dates, time_difference):
        """
        Filters out dates within time_difference, preserving only the oldest date.

        :param dates: a list of datetime objects
        :param time_difference: a datetime.timedelta representing the time difference threshold
        :return: an ordered list of datetimes d1<=d2<=...<=dn such that d[i+1]-di > time_difference
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

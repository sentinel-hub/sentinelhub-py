"""
Module for working with Sentinel Hub OGC services
"""

import logging
import shapely

from urllib.parse import urlencode
from base64 import b64encode

from .time_utils import parse_time_interval
from .download import DownloadRequest
from .constants import DataSource, MimeType, CRS, OgcConstants, CustomUrlParam
from .config import SHConfig
from .ogc import OgcService
from .common import BBox, Geometry

LOGGER = logging.getLogger(__name__)


class FisService(OgcService):
    """Sentinel Hub OGC services class for providing FIS data

    Intermediate layer between FIS requests and the Sentinel Hub FIS services.

    :param base_url: base url of Sentinel Hub's OGC services. If ``None``, the url specified in the configuration
                    file is taken.
    :type base_url: str or None
    :param instance_id: user's instance id granting access to Sentinel Hub's OGC services. If ``None``, the instance id
                        specified in the configuration file is taken.
    :type instance_id: str or None
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_request(self, request):
        """ Get download requests

        Create a list of DownloadRequests for all Sentinel-2 acquisitions within request's time interval and
        acceptable cloud coverage.


        :param request: QGC-type request with specified bounding box, time interval, and cloud coverage for specific
                        product.
        :type request: OgcRequest or GeopediaRequest
        :return: list of DownloadRequests
        """

        return [DownloadRequest(url=self.get_url(request, geometry),
                                filename=self.get_filename(request, geometry),
                                data_type=MimeType.JSON,
                                headers=OgcConstants.HEADERS)
                for geometry in request.geometry_list]

    def get_base_url(self, request):
        """ Get base URL

        :param request:
        :return:
        """
        url = self.base_url + request.service_type.value

        if hasattr(request, 'data_source') and request.data_source.is_uswest_source():
            url = 'https://services-uswest2.sentinel-hub.com/ogc/{}'.format(request.service_type.value)

        if hasattr(request, 'data_source') and request.data_source not in DataSource.get_available_sources():
            raise ValueError("{} is not available for service at ogc_base_url={}".format(request.data_source,
                                                                                         SHConfig().ogc_base_url))
        return url

    def get_fis_parameters(self, request, geometry):
        """ Get FIS specific parameters.
        :param request:
        :param geometry:
        :return:
        """
        params = {
            'SERVICE': request.service_type.value,
            'CRS': CRS.ogc_string(geometry.get_crs()),
            'LAYER': request.layer,
            'RESOLUTION': request.resolution,
            'TIME': self.get_dates(request)
        }

        if isinstance(geometry, Geometry):
            params['GEOMETRY'] = geometry.to_wkt()
        elif isinstance(geometry, BBox):
            params['BBOX'] = geometry.__str__(reverse=True) if geometry.get_crs() is CRS.WGS84 else str(geometry)

        if hasattr(request, 'bins') and request.bins:
            params['BINS'] = request.bins

        if request.histogram_type:
            params['TYPE'] = request.histogram_type.value

        return params

    def get_common_parameters(self, request):
        """ Get common parameters
        :param request:
        :return:
        """
        params = {}
        if hasattr(request, 'maxcc'):
            params['MAXCC'] = 100.0 * request.maxcc

        if hasattr(request, 'custom_url_params') and request.custom_url_params is not None:
            params = {**params,
                      **{k.value: str(v) for k, v in request.custom_url_params.items()}}

            if CustomUrlParam.EVALSCRIPT.value in params:
                evalscript = params[CustomUrlParam.EVALSCRIPT.value]
                params[CustomUrlParam.EVALSCRIPT.value] = b64encode(evalscript.encode()).decode()

            if CustomUrlParam.GEOMETRY.value in params and request.bbox.get_crs() is CRS.WGS84:
                geometry = shapely.wkt.loads(params[CustomUrlParam.GEOMETRY.value])
                geometry = shapely.ops.transform(lambda x, y: (y, x), geometry)

                params[CustomUrlParam.GEOMETRY.value] = geometry.wkt

        return params

    def get_url(self, request, geometry):
        """
        Returns url to Sentinel Hub's OGC service for the product specified by the OgcRequest and date.
        :param request: OGC-type request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest
        :param geometry: (multi)polygon
        :type geometry: common.Geometry
        :return: url to Sentinel Hub's OGC service for this product.
        :rtype: str
        """
        params = {}
        params = {**params, **self.get_fis_parameters(request, geometry)}
        params = {**params, **self.get_common_parameters(request)}

        url = self.get_base_url(request)
        authority = self.instance_id
        url = '{}/{}?{}'.format(url, authority, urlencode(params))
        return url

    @staticmethod
    def get_filename(request, geometry):
        """ Returns filepath

    Returns the filename's location on disk where data is or is going to be stored.
    The files are stored in the folder specified by the user when initialising OGC-type
    of request. The name of the file has the following structure:

    {service_type}_{layer}_{geometry}_{crs}_{start_time}_{end_time}_{resolution}_{custom_url_param}_
    {custom_url_param_val}.json

    :param request: FIS request
    :param geometry: geometry object
    :type: BBox or Geometry
    :return: filename for this request
    :rtype: str
    """
        if isinstance(geometry, Geometry):
            geometry_string = geometry.to_wkt()
        elif isinstance(geometry, BBox):
            geometry_string = geometry.__str__(reverse=True) if geometry.get_crs() is CRS.WGS84 else str(geometry)
        else:
            geometry_string = ""

        filename = '_'.join([str(request.service_type.value),
                             request.layer,
                             geometry_string,
                             CRS.ogc_string(geometry.get_crs()),
                             FisService.get_dates(request),
                             request.resolution])

        if hasattr(request, 'custom_url_params') and request.custom_url_params is not None:
            for param, value in sorted(request.custom_url_params.items(),
                                       key=lambda parameter_item: parameter_item[0].value):
                filename = '_'.join([filename, param.value, str(value)])

        for char in [' ', '/', '\\', '|', ';', ':', '\n', '\t', '(', ')']:
            filename = filename.replace(char, '')

        suffix = ".json"
        filename = '.'.join([filename[:254 - len(suffix)], suffix])

        return filename

    @staticmethod
    def get_dates(request):
        """ Get date in right form date/date

        :param request: OGC-type request
        :type request: FisRequest
        :return: date or date/date
        """

        date_interval = parse_time_interval(request.time)
        return '{}/{}'.format(date_interval[0], date_interval[1])

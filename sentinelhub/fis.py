"""
Module for working with Sentinel Hub OGC services
"""

import logging

from .time_utils import parse_time_interval
from .download import DownloadRequest
from .constants import MimeType, CRS, OgcConstants
from .ogc import OgcImageService
from .common import BBox, Geometry

LOGGER = logging.getLogger(__name__)


class FisService(OgcImageService):
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

        return [DownloadRequest(url=self.get_url(request=request, geometry=geometry),
                                filename=self.get_filename(request, geometry),
                                data_type=MimeType.JSON,
                                headers=OgcConstants.HEADERS)
                for geometry in request.geometry_list]

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
        date_interval = parse_time_interval(request.time)
        time = '{}/{}'.format(date_interval[0], date_interval[1])
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
                             time,
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

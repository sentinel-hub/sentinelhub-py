"""
Module for working with Geopedia OGC services
"""

import logging

from urllib.parse import urlencode

from .download import DownloadRequest
from .constants import ServiceType, MimeType, CRS
from .config import SGConfig

LOGGER = logging.getLogger(__name__)


class GeopediaService:
    """ The class for Geopedia OGC services

    :param base_url: Base url of Geopedia's OGC services. If ``None``, the url specified in the configuration
                    file is taken.
    :type base_url: str or None
    """
    def __init__(self, base_url=None):
        self.base_url = SGConfig().gpd_base_url if base_url is None else base_url


class GeopediaImageService(GeopediaService):
    """Geopedia OGC services class for providing image data

    :param base_url: Base url of Geopedia's OGC services. If ``None``, the url specified in the configuration
                    file is taken.
    :type base_url: str or None
    """
    def __init__(self, **kwargs):
        super(GeopediaImageService, self).__init__(**kwargs)

    def get_request(self, request):
        """ Get download requests

        Create a list of DownloadRequests.

        :param request: Geopedia request with specified bounding box for specific product/layer.
        :type request: GeopediaRequest
        :return: list of DownloadRequests
        """
        return [DownloadRequest(url=self.get_url(request),
                                filename=self.get_filename(request),
                                data_type=request.image_format)]

    def get_url(self, request):
        """ Returns url to Geopedia's OGC service for the product specified by the GeopediaRequest.

        :param request: Geopedia request with specified bounding box for specific product/layer.
        :type request: GeopediaRequest
        :return: Url to Sentinel Hub's OGC service for this product.
        :rtype: str
        """
        url = self.base_url + request.service_type.value

        params = {'SERVICE': request.service_type.value,
                  'BBOX': str(request.bbox),
                  'FORMAT': MimeType.get_string(request.image_format),
                  'CRS': CRS.ogc_string(request.bbox.get_crs())}

        if request.service_type is ServiceType.WMS:
            params = {**params,
                      **{
                          'WIDTH': request.size_x,
                          'HEIGHT': request.size_y,
                          'LAYERS': request.layer,
                          'REQUEST': 'GetMap'
                      }}
        elif request.service_type is ServiceType.WCS:
            params = {**params,
                      **{
                          'RESX': request.size_x,
                          'RESY': request.size_y,
                          'COVERAGE': request.layer,
                          'REQUEST': 'GetCoverage'
                      }}

        return '{}/{}?{}'.format(url, request.theme, urlencode(params))

    @staticmethod
    def get_filename(request):
        """ Get filename location

        Returns the filename's location on disk where data is or is going to be stored.
        The files are stored in the folder specified by the user when initialising OGC-type
        of request. The name of the file has the following structure:

        {service_type}_{layer}_{crs}_{bbox}.{image_format}

        In case of `TIFF_d32f` a `'_tiff_depth32f'` is added at the end of the filename (before format suffix)
        to differentiate it from 16-bit float tiff.

        :param request: OGC request with specified bounding box, cloud coverage for specific product.
        :type request: OgcRequest
        :return: Filename for this request and date
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
                             bbox_str])

        LOGGER.debug("filename=%s", filename)

        if fmt:
            filename = '_'.join([filename, fmt])

        filename = '.'.join([filename, suffix])

        return filename

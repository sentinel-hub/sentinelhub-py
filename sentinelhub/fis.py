"""
Module for working with Sentinel Hub FIS service
"""
import logging

from .download import DownloadRequest
from .constants import MimeType, SHConstants, RequestType
from .ogc import OgcImageService

LOGGER = logging.getLogger(__name__)


class FisService(OgcImageService):
    """ Sentinel Hub OGC services class for providing FIS data

    Intermediate layer between FIS requests and the Sentinel Hub FIS services.
    """
    def get_request(self, request):
        """ Get download requests

        Create a list of DownloadRequests for all Sentinel-2 acquisitions within request's time interval and
        acceptable cloud coverage.

        :param request: OGC-type request with specified bounding box, time interval, and cloud coverage for specific
            product.
        :type request: OgcRequest or GeopediaRequest
        :return: list of DownloadRequests
        """

        return [self._create_request(request=request, geometry=geometry) for geometry in request.geometry_list]

    def _create_request(self, request, geometry):

        url = self.get_base_url(request)

        headers = {'Content-Type': MimeType.JSON.get_string(), **SHConstants.HEADERS}

        post_data = {**self._get_common_url_parameters(request), **self._get_fis_parameters(request, geometry)}
        post_data = {k.lower(): v for k, v in post_data.items()}  # lowercase required on SH service

        return DownloadRequest(url=f'{url}/{self.config.instance_id}',
                               post_values=post_data,
                               data_type=MimeType.JSON, headers=headers,
                               request_type=RequestType.POST)

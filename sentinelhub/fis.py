"""
Module for working with Sentinel Hub FIS service
"""
import logging

from .time_utils import parse_time_interval
from .download import DownloadRequest
from .constants import MimeType, CRS, SHConstants, RequestType
from .ogc import OgcImageService
from .geometry import Geometry

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
        authority = self.instance_id if hasattr(self, 'instance_id') else request.theme
        headers = {'Content-Type': MimeType.JSON.get_string(), **SHConstants.HEADERS}

        post_data = {**self._get_common_url_parameters(request), **self._get_fis_parameters(request, geometry)}
        post_data = {k.lower(): v for k, v in post_data.items()}  # lowercase required on SH service

        return DownloadRequest(url='{}/{}'.format(url, authority),
                               post_values=post_data,
                               filename=self.get_filename(request, geometry),
                               data_type=MimeType.JSON, headers=headers,
                               request_type=RequestType.POST)

    @staticmethod
    def get_filename(request, geometry):
        """ Returns the filename location on disk where data is or is going to be stored.
        The files are stored in the folder specified by the user when initialising OGC-type
        of request. The name of the file has the following structure:

        {service_type}_{layer}_{geometry}_{crs}_{start_time}_{end_time}_{resolution}_{bins}_{histogram_type}_
        \\*{custom_url_params}.json

        :param request: FIS request
        :type request: FisRequest
        :param geometry: geometry object
        :type geometry: BBox or Geometry
        :return: filename for this request
        :rtype: str
        """
        date_interval = parse_time_interval(request.time)
        geometry_string = geometry.wkt if isinstance(geometry, Geometry) else str(geometry)

        filename = '_'.join([
            str(request.service_type.value),
            request.layer,
            geometry_string,
            CRS.ogc_string(geometry.crs),
            '{}_{}'.format(date_interval[0], date_interval[1]),
            request.resolution,
            str(request.bins) if request.bins else '',
            request.histogram_type.value if request.histogram_type else ''
        ])

        filename = OgcImageService.filename_add_custom_url_params(filename, request)

        return OgcImageService.finalize_filename(filename, MimeType.JSON)

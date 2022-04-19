"""
Data request interface for Geopedia services
"""

from abc import abstractmethod

from ..base import DataRequest
from ..constants import CRS, MimeType, ServiceType
from ..download import DownloadClient
from .core import GeopediaImageService, GeopediaWmsService


class GeopediaRequest(DataRequest):
    """The base class for Geopedia requests where all common parameters are defined."""

    def __init__(self, layer, service_type, *, bbox=None, theme=None, image_format=MimeType.PNG, **kwargs):
        """
        :param layer: Geopedia layer which contains requested data
        :type layer: str
        :param service_type: Type of the service, supported are ``ServiceType.WMS`` and ``ServiceType.IMAGE``
        :type service_type: constants.ServiceType
        :param bbox: Bounding box of the requested data
        :type bbox: geometry.BBox
        :param theme: Geopedia's theme endpoint string for which the layer is defined. Only required by WMS service.
        :type theme: str
        :param image_format: Format of the returned image by the Sentinel Hub's WMS getMap service. Default is
            ``constants.MimeType.PNG``.
        :type image_format: constants.MimeType
        :param data_folder: Location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.layer = layer
        self.service_type = service_type

        self.bbox = bbox
        if bbox.crs is not CRS.POP_WEB:
            raise ValueError(
                f"Geopedia Request at the moment supports only bounding boxes with coordinates in {CRS.POP_WEB}"
            )

        self.theme = theme
        self.image_format = MimeType(image_format)

        super().__init__(DownloadClient, **kwargs)

    @abstractmethod
    def create_request(self):
        raise NotImplementedError


class GeopediaWmsRequest(GeopediaRequest):
    """Web Map Service request class for Geopedia

    Creates an instance of Geopedia's WMS (Web Map Service) GetMap request, which provides access to WMS layers in
    Geopedia.
    """

    def __init__(self, layer, theme, bbox, *, width=None, height=None, **kwargs):
        """
        :param layer: Geopedia layer which contains requested data
        :type layer: str
        :param theme: Geopedia's theme endpoint string for which the layer is defined.
        :type theme: str
        :param bbox: Bounding box of the requested data
        :type bbox: geometry.BBox
        :param width: width (number of columns) of the returned image (array)
        :type width: int or None
        :param height: height (number of rows) of the returned image (array)
        :type height: int or None
        :param image_format: Format of the returned image by the Sentinel Hub's WMS getMap service. Default is
            ``constants.MimeType.PNG``.
        :type image_format: constants.MimeType
        :param data_folder: Location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.size_x = width
        self.size_y = height

        super().__init__(layer=layer, theme=theme, bbox=bbox, service_type=ServiceType.WMS, **kwargs)

    def create_request(self):
        """Set download requests

        Create a list of DownloadRequests for all Sentinel-2 acquisitions within request's time interval and
        acceptable cloud coverage.
        """
        gpd_service = GeopediaWmsService(config=self.config)
        self.download_list = gpd_service.get_request(self)


class GeopediaImageRequest(GeopediaRequest):
    """Request to access data in a Geopedia vector / raster layer."""

    def __init__(self, *, image_field_name, keep_image_names=True, gpd_session=None, **kwargs):
        """
        :param image_field_name: Name of the field in the data table which holds images
        :type image_field_name: str
        :param keep_image_names: If `True` images will be saved with the same names as in Geopedia otherwise Geopedia
            hashes will be used as names. If there are multiple images with the same names in the Geopedia layer this
            parameter should be set to `False` to prevent images being overwritten.
        :type keep_image_names: bool
        :param layer: Geopedia layer which contains requested data
        :type layer: str
        :param bbox: Bounding box of the requested data
        :type bbox: geometry.BBox
        :param image_format: Format of the returned image by the Sentinel Hub's WMS getMap service. Default is
            ``constants.MimeType.PNG``.
        :type image_format: constants.MimeType
        :param gpd_session: Optional parameter for specifying a custom Geopedia session, which can also contain login
            credentials. This can be used for accessing private Geopedia layers. By default, it is set to `None` and a
            basic Geopedia session without credentials will be created.
        :type gpd_session: GeopediaSession or None
        :param data_folder: Location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.image_field_name = image_field_name
        self.keep_image_names = keep_image_names
        self.gpd_session = gpd_session

        self.gpd_iterator = None

        super().__init__(service_type=ServiceType.IMAGE, **kwargs)

    def create_request(self, reset_gpd_iterator=False):
        """Set a list of download requests

        Set a list of DownloadRequests for all images that are under the
        given property of the Geopedia's Vector layer.

        :param reset_gpd_iterator: When re-running the method this flag is used to reset/keep existing ``gpd_iterator``
            (i.e. instance of ``GeopediaFeatureIterator`` class). If the iterator is not reset you don't have to
            repeat a service call but tiles and dates will stay the same.
        :type reset_gpd_iterator: bool
        """
        if reset_gpd_iterator:
            self.gpd_iterator = None

        gpd_service = GeopediaImageService(config=self.config)
        self.download_list = gpd_service.get_request(self)
        self.gpd_iterator = gpd_service.get_gpd_iterator()

    def get_items(self):
        """Returns iterator over info about data used for this request

        :return: Iterator of dictionaries containing info about data used in
                 this request.
        :rtype: Iterator[dict] or None
        """
        return self.gpd_iterator

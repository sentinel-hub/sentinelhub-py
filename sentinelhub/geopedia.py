"""
Module for working with Geopedia OGC services
"""

import logging

from .ogc import OgcImageService
from .config import SHConfig

LOGGER = logging.getLogger(__name__)


class GeopediaService:
    """ The class for Geopedia OGC services

    :param base_url: Base url of Geopedia's OGC services. If ``None``, the url specified in the configuration
                    file is taken.
    :type base_url: str or None
    """
    def __init__(self, base_url=None):
        self.base_url = SHConfig().gpd_base_url if base_url is None else base_url


class GeopediaImageService(GeopediaService, OgcImageService):
    """Geopedia OGC services class for providing image data. Most of the methods are inherited from
    `sentinelhub.ogc.OgcImageService` class.

    :param base_url: Base url of Geopedia's OGC services. If ``None``, the url specified in the configuration
                    file is taken.
    :type base_url: str or None
    """
    def __init__(self, **kwargs):
        super(GeopediaImageService, self).__init__(**kwargs)

    def get_dates(self, request):
        """ Geopedia does not support date queries

        :param request: OGC-type request
        :type request: WmsRequest or WcsRequest
        :return: Undefined date
        :rtype: [None]
        """
        return [None]

    def get_wfs_iterator(self):
        """ This method is inherited from OgcImageService but is not implemented.
        """
        raise NotImplementedError

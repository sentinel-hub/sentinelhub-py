"""
Module for working with Geopedia OGC services
"""

import logging

from shapely.geometry import shape as geo_shape

from .ogc import OgcImageService, MimeType
from .config import SHConfig
from .download import DownloadRequest, get_json
from .constants import CRS

LOGGER = logging.getLogger(__name__)


class GeopediaService:
    """ The class for Geopedia OGC services

    :param base_url: Base url of Geopedia's OGC services. If ``None``, the url specified in the configuration
                    file is taken.
    :type base_url: str or None
    """
    def __init__(self, base_url=None):
        self.base_url = SHConfig().gpd_base_url if base_url is None else base_url


class GeopediaWmsService(GeopediaService, OgcImageService):
    """Geopedia OGC services class for providing image data. Most of the methods are inherited from
    `sentinelhub.ogc.OgcImageService` class.

    :param base_url: Base url of Geopedia's OGC services. If ``None``, the url specified in the configuration
                    file is taken.
    :type base_url: str or None
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

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


class GeopediaImageService(GeopediaService):
    """Service class that provides images from a Geopedia vector layer.

    :param base_url: Base url of Geopedia's OGC services. If ``None``, the url
                     specified in the configuration file is taken.
    :type base_url: str or None
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.gpd_iterator = None

    def get_request(self, request):
        """Get download requests

        Get a list of DownloadRequests for all data that are under the given
        property of the Geopedia Vector layer.

        :return: list of DownloadRequests
        """
        return [DownloadRequest(url=self._get_url(item),
                                filename=self._get_filename(request, item),
                                data_type=request.image_format)
                for item in self._get_items(request)]

    def _get_items(self, request):
        self.gpd_iterator = GeopediaFeatureIterator(request.layer,
                                                    bbox=request.bbox,
                                                    base_url=self.base_url)
        prop_iter = self.gpd_iterator.get_property_iterator(request.prop)
        items = []

        for prop_items in prop_iter:
            # an image property can have multiple images
            for item in prop_items:
                if not item['mimeType'].startswith('image/'):
                    continue

                mime_type = MimeType.from_string(item['mimeType'][6:])

                if mime_type == request.image_format:
                    items.append(item)
                    break

        return items

    @staticmethod
    def _get_url(item):
        return item.get('objectPath')

    @staticmethod
    def _get_filename(request, item):
        filename = '_'.join([request.layer,
                             str(request.bbox.get_crs()),
                             str(request.bbox).replace(',', '_'),
                             item['objectPath'].split("/")[-1]])

        LOGGER.debug("filename=%s", filename)

        for char in [' ', '/', '\\', '|', ';', ':', '\n', '\t']:
            filename = filename.replace(char, '')

        suffix = str(request.image_format.value)
        filename = '.'.join([filename[:254 - len(suffix)], suffix])
        return filename  # Even in UNIX systems filename must have at most 255 bytes

    def get_gpd_iterator(self):
        """Returns iterator over info about data used for the
        GeopediaVectorRequest

        :return: Iterator of dictionaries containing info about data used in
                 the request.
        :rtype: Iterator[dict] or None
        """
        return self.gpd_iterator


class GeopediaFeatureIterator(GeopediaService):
    """Iterator for Geopedia Vector Service

    :type layer: str
    :param bbox: Bounding box of the requested image. Its coordinates must be
                 in the CRS.POP_WEB (EPSG:3857) coordinate system.
    :type bbox: common.BBox
    :param base_url: Base url of Geopedia's OGC services. If ``None``, the url
                     specified in the configuration file is taken.
    :type base_url: str or None
    """
    def __init__(self, layer, bbox=None, **kwargs):
        super().__init__(**kwargs)

        self.layer = layer

        self.query = {}
        if bbox is not None:

            if bbox.crs is not CRS.POP_WEB:
                raise ValueError('Geopedia Request at the moment supports only CRS = {}'.format(CRS.POP_WEB))

            self.query['filterExpression'] = 'bbox({},"EPSG:3857")'.format(bbox)

        self.features = []
        self.index = 0

        self.session_url = '{}rest/data/v1/session/create?locale=en'.format(self.base_url)
        self.next_page_url = '{}rest/data/v2/search/tables/{}/features'.format(self.base_url, layer)

    def __iter__(self):
        self.index = 0

        return self

    def __next__(self):
        if self.index == len(self.features):
            self._fetch_features()

        if self.index < len(self.features):
            self.index += 1
            return self.features[self.index - 1]

        raise StopIteration

    def _fetch_features(self):
        if self.next_page_url is None:
            return

        response = get_json(self.next_page_url,
                            post_values=self.query,
                            headers=self._get_headers())

        self.features.extend(response['features'])
        self.next_page_url = response['pagination']['next']

    def _get_headers(self):
        headers = {'X-GPD-Session': self._get_session_id()}

        return headers

    def _get_session_id(self):
        session_id = get_json(self.session_url)['sessionId']

        return session_id

    def get_geometry_iterator(self):
        for feature in self:
            yield geo_shape(feature['geometry'])

    def get_property_iterator(self, prop):
        for feature in self:
            yield feature['properties'].get(prop)

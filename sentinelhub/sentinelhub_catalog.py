"""
A client interface for Sentinel Hub Catalog API
"""
from .config import SHConfig
from .constants import CRS
from .data_collections import DataCollection
from .download.sentinelhub_client import SentinelHubDownloadClient
from .sentinelhub_batch import _iter_pages, _remove_undefined_params  # TODO: move to utils
from .time_utils import parse_time_interval


class SentinelHubCatalog:
    """ The main class for interacting with Sentinel Hub Catalog API
    """
    def __init__(self, base_url=None, config=None):
        self.config = config or SHConfig()

        base_url = base_url or self.config.sh_base_url
        base_url = base_url.rstrip('/')
        self.catalog_url = f'{base_url}/api/v1/catalog'

        self.client = SentinelHubDownloadClient(config=self.config)

    def get_info(self):
        """ Provides the main information that define Sentinel Hub Catalog API

        :return: A service payload with information
        :rtype: dict
        """
        return self.client.get_json(self.catalog_url)

    def get_conformance(self):
        """ Get information about specifications that this API conforms to

        :return: A service payload with information
        :rtype: dict
        """
        url = f'{self.catalog_url}/conformance'
        return self.client.get_json(url)

    def get_collections(self):
        """ Provides a list of collections that are available to a user

        :return: A list of collections with information
        :rtype: list(dict)
        """
        url = f'{self.catalog_url}/collections'
        return self.client.get_json(url, use_session=True)['collections']

    def get_collection_info(self, collection):
        """ Provides information about given collection

        :param collection: A data collection object or a collection ID
        :type collection: DataCollection or str
        :return: Information about a collection
        :rtype: dict
        """
        collection_id = self._parse_collection_id(collection)
        url = f'{self.catalog_url}/collections/{collection_id}'
        return self.client.get_json(url, use_session=True)

    def get_feature(self, collection, feature_id):
        """ Provides information about a single feature in a collection

        :param collection: A data collection object or a collection ID
        :type collection: DataCollection or str
        :param feature_id: A feature ID
        :type feature_id: str
        :return: Information about a feature in a collection
        :rtype: dict
        """
        collection_id = self._parse_collection_id(collection)
        url = f'{self.catalog_url}/collections/{collection_id}/items/{feature_id}'
        return self.client.get_json(url, use_session=True)

    def search(self, collection, *, time, bbox=None, geometry=None, ids=None, limit=100, query=None, fields=None,
               distinct=None, **kwargs):
        """ Catalog STAC search

        :param collection: A data collection object or a collection ID
        :type collection: DataCollection or str
        TODO: can you use intersects without bbox?
        TODO: time_interval instead of time? what does return if a single timestamp is given?
        """
        url = f'{self.catalog_url}/search'

        collection_id = self._parse_collection_id(collection)
        start_time, end_time = parse_time_interval(time)
        bbox = bbox.transform(CRS.WGS84) if bbox else None
        geometry = geometry.transform(CRS.WGS84) if geometry else None

        params = _remove_undefined_params({
            'collections': [collection_id],
            'bbox': list(bbox),
            'datetime': f'{start_time}Z/{end_time}Z',
            'intersects': geometry.geojson,
            'ids': ids,
            'limit': limit,
            'query': query,
            'fields': fields,
            'distinct': distinct,
            **kwargs
        })

        return CatalogSearchIterator(self.client, url, **params)

    @staticmethod
    def _parse_collection_id(collection):
        """ Extracts catalog collection id from an object defining a collection
        """
        if isinstance(collection, DataCollection):
            return collection.catalog_id
        if isinstance(collection, str):
            return collection
        raise ValueError(f'Expected either a DataCollection object or a collection id string, got {collection}')


class CatalogSearchIterator:

    def __init__(self, client, url, **params):
        self.client = client
        self.url = url
        self.params = params

        self.index = 0
        self.features = []
        self.next = None
        self.finished = False

    def __iter__(self):
        """ Iteration method

        :return: the iterator class itself
        :rtype: CatalogSearchIterator
        """
        self.index = 0
        return self

    def __next__(self):
        """ Next method

        :return: dictionary containing info about product tiles
        :rtype: dict
        """
        while self.index >= len(self.features) and not self.finished:
            self._fetch_features()

        if self.index < len(self.features):
            self.index += 1
            return self.features[self.index - 1]

        raise StopIteration

    def _fetch_features(self):
        """ Collects (more) results from the service
        """
        if self.next is not None:
            params = {
                **self.params,
                'next': self.next
            }
        else:
            params = self.params

        results = self.client.get_json(self.url, post_values=params, use_session=True)

        new_features = results['features']
        self.features.extend(new_features)

        self.next = results['context'].get('next')
        self.finished = self.next is None or not new_features

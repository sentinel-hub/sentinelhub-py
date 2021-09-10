"""
A client interface for Sentinel Hub Catalog API
"""
from .data_collections import DataCollection, OrbitDirection
from .geometry import Geometry, CRS
from .sh_utils import SentinelHubService, FeatureIterator, remove_undefined
from .time_utils import parse_time_interval, serialize_time, parse_time


class SentinelHubCatalog(SentinelHubService):
    """ The main class for interacting with Sentinel Hub Catalog API

    For more details about certain endpoints and parameters check
    `Catalog API documentation <https://docs.sentinel-hub.com/api/latest/api/catalog>`_.
    """
    @staticmethod
    def _get_service_url(base_url):
        """ Provides URL to Catalog API
        """
        return f'{base_url}/api/v1/catalog'

    def get_info(self):
        """ Provides the main information that define Sentinel Hub Catalog API

        `Catalog API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getLandingPage>`__

        :return: A service payload with information
        :rtype: dict
        """
        return self.client.get_json(self.service_url)

    def get_conformance(self):
        """ Get information about specifications that this API conforms to

        `Catalog API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/getConformanceDeclaration>`__

        :return: A service payload with information
        :rtype: dict
        """
        url = f'{self.service_url}/conformance'
        return self.client.get_json(url)

    def get_collections(self):
        """ Provides a list of collections that are available to a user

        `Catalog API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getCollections>`__

        :return: A list of collections with information
        :rtype: list(dict)
        """
        url = f'{self.service_url}/collections'
        return self.client.get_json(url, use_session=True)['collections']

    def get_collection(self, collection):
        """ Provides information about given collection

        `Catalog API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/describeCollection>`__

        :param collection: A data collection object or a collection ID
        :type collection: DataCollection or str
        :return: Information about a collection
        :rtype: dict
        """
        collection_id = self._parse_collection_id(collection)
        url = f'{self.service_url}/collections/{collection_id}'
        return self.client.get_json(url, use_session=True)

    def get_feature(self, collection, feature_id):
        """ Provides information about a single feature in a collection

        `Catalog API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getFeature>`__

        :param collection: A data collection object or a collection ID
        :type collection: DataCollection or str
        :param feature_id: A feature ID
        :type feature_id: str
        :return: Information about a feature in a collection
        :rtype: dict
        """
        collection_id = self._parse_collection_id(collection)
        url = f'{self.service_url}/collections/{collection_id}/items/{feature_id}'
        return self.client.get_json(url, use_session=True)

    def search(self, collection, *, time, bbox=None, geometry=None, ids=None, query=None, fields=None, distinct=None,
               limit=100, **kwargs):
        """ Catalog STAC search

        `Catalog API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/postSearchSTAC>`__

        :param collection: A data collection object or a collection ID
        :type collection: DataCollection or str
        :param time: A time interval or a single time. It can either be a string in form  YYYY-MM-DDThh:mm:ss or
            YYYY-MM-DD or a datetime object
        :type time: (str, str) or (datetime, datetime) or str or datetime
        :param bbox: A search bounding box, it will always be reprojected to WGS 84 before being sent to the service.
            Re-projection will be done with BBox.transform_bounds method which can produce a slightly larger bounding
            box. If that is a problem then transform a BBox object into a Geometry object and search with geometry
            parameter instead.
        :type bbox: BBox
        :param geometry: A search geometry, it will always reprojected to WGS 84 before being sent to the service.
            This parameter is defined with parameter `intersects` at the service.
        :type geometry: Geometry
        :param ids: A list of feature ids as defined in service documentation
        :type ids: list(str)
        :param query: A STAC query described in Catalog API documentation
        :type query: dict
        :param fields: A dictionary of fields to include or exclude described in Catalog API documentation
        :type fields: dict
        :param distinct: A special query attribute described in Catalog API documentation
        :type distinct: str
        :param limit: A number of results to return per each request. At the end iterator will always provide all
            results the difference is only in how many requests it will have to make in the background.
        :type limit: int
        :param kwargs: Any other parameters that will be passed directly to the service
        """
        url = f'{self.service_url}/search'

        collection_id = self._parse_collection_id(collection)
        start_time, end_time = serialize_time(parse_time_interval(time, allow_undefined=True), use_tz=True)

        if bbox and bbox.crs is not CRS.WGS84:
            bbox = bbox.transform_bounds(CRS.WGS84)
        if geometry and geometry.crs is not CRS.WGS84:
            geometry = geometry.transform(CRS.WGS84)

        _query = self._get_data_collection_filters(collection)
        if query:
            _query.update(query)

        payload = remove_undefined({
            'collections': [collection_id],
            'datetime': f'{start_time}/{end_time}' if time else None,
            'bbox': list(bbox) if bbox else None,
            'intersects': geometry.get_geojson(with_crs=False) if geometry else None,
            'ids': ids,
            'query': _query,
            'fields': fields,
            'distinct': distinct,
            'limit': limit,
            **kwargs
        })

        return CatalogSearchIterator(self.client, url, payload)

    @staticmethod
    def _parse_collection_id(collection):
        """ Extracts catalog collection id from an object defining a collection
        """
        if isinstance(collection, DataCollection):
            return collection.catalog_id
        if isinstance(collection, str):
            return collection
        raise ValueError(f'Expected either a DataCollection object or a collection id string, got {collection}')

    @staticmethod
    def _get_data_collection_filters(data_collection):
        """ Builds a dictionary of query filters for catalog API from a data collection definition
        """
        filters = {}

        if isinstance(data_collection, str):
            return filters

        if data_collection.swath_mode:
            filters['sar:instrument_mode'] = {'eq': data_collection.swath_mode.upper()}

        if data_collection.polarization:
            filters['polarization'] = {'eq': data_collection.polarization.upper()}

        if data_collection.resolution:
            filters['resolution'] = {'eq': data_collection.resolution.upper()}

        if data_collection.orbit_direction and data_collection.orbit_direction.upper() != OrbitDirection.BOTH:
            filters['sat:orbit_state'] = {'eq': data_collection.orbit_direction.lower()}

        if data_collection.timeliness:
            filters['timeliness'] = {'eq': data_collection.timeliness}

        return filters


class CatalogSearchIterator(FeatureIterator):
    """ Searches a catalog with a given query and provides results
    """

    def _fetch_features(self):
        """ Collects more results from the service
        """
        payload = remove_undefined({
            **self.params,
            'next': self.next
        })

        results = self.client.get_json(self.url, post_values=payload, use_session=True)

        self.next = results['context'].get('next')
        new_features = results['features']
        self.finished = self.next is None or not new_features

        return new_features

    def get_timestamps(self):
        """ Provides features timestamps

        :return: A list of sensing times
        :rtype: list(datetime.datetime)
        """
        return [parse_time(feature['properties']['datetime']) for feature in self]

    def get_geometries(self):
        """ Provides features geometries

        :return: A list of geometry objects with CRS
        :rtype: list(Geometry)
        """
        return [Geometry.from_geojson(feature['geometry']) for feature in self]

    def get_ids(self):
        """ Provides features IDs

        :return: A list of IDs
        :rtype: list(str)
        """
        return [feature['id'] for feature in self]

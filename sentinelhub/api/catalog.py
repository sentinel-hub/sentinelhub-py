"""
A client interface for `Sentinel Hub Catalog API <https://docs.sentinel-hub.com/api/latest/api/catalog>`__.
"""
import datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Union

from ..base import FeatureIterator
from ..data_collections import DataCollection, OrbitDirection
from ..geometry import CRS, BBox, Geometry
from ..time_utils import parse_time, parse_time_interval, serialize_time
from ..types import JsonDict, Literal, RawTimeIntervalType, RawTimeType
from .base import SentinelHubService
from .utils import remove_undefined


class SentinelHubCatalog(SentinelHubService):
    """The main class for interacting with Sentinel Hub Catalog API

    For more details about certain endpoints and parameters check
    `Catalog API documentation <https://docs.sentinel-hub.com/api/latest/api/catalog>`__.
    """

    @staticmethod
    def _get_service_url(base_url: str) -> str:
        """Provides URL to Catalog API"""
        return f"{base_url}/api/v1/catalog/1.0.0"

    def get_info(self) -> JsonDict:
        """Provides the main information that define Sentinel Hub Catalog API

        `Catalog API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getLandingPage>`__

        :return: A service payload with information
        """
        return self.client.get_json_dict(self.service_url)

    def get_conformance(self) -> JsonDict:
        """Get information about specifications that this API conforms to

        `Catalog API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/getConformanceDeclaration>`__

        :return: A service payload with information
        """
        return self.client.get_json_dict(f"{self.service_url}/conformance")

    def get_collections(self) -> List[JsonDict]:
        """Provides a list of collections that are available to a user

        `Catalog API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getCollections>`__

        :return: A list of collections with information
        """
        return self.client.get_json_dict(f"{self.service_url}/collections", use_session=True)["collections"]

    def get_collection(self, collection: Union[DataCollection, str]) -> JsonDict:
        """Provides information about given collection

        `Catalog API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/describeCollection>`__

        :param collection: A data collection object or a collection ID
        :return: Information about a collection
        """
        collection_id = self._parse_collection_id(collection)
        url = f"{self.service_url}/collections/{collection_id}"
        return self.client.get_json_dict(url, use_session=True)

    def get_feature(self, collection: DataCollection, feature_id: str) -> JsonDict:
        """Provides information about a single feature in a collection

        `Catalog API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getFeature>`__

        :param collection: A data collection object or a collection ID
        :param feature_id: A feature ID
        :return: Information about a feature in a collection
        """
        collection_id = self._parse_collection_id(collection)
        url = f"{self.service_url}/collections/{collection_id}/items/{feature_id}"
        return self.client.get_json_dict(url, use_session=True)

    def search(
        self,
        collection: Union[DataCollection, str],
        *,
        time: Union[RawTimeType, RawTimeIntervalType] = None,
        bbox: Optional[BBox] = None,
        geometry: Optional[Geometry] = None,
        ids: Optional[List[str]] = None,
        filter: Union[None, str, JsonDict] = None,  # pylint: disable=redefined-builtin
        filter_lang: Literal["cql2-text", "cql2-json"] = "cql2-text",
        filter_crs: Optional[str] = None,
        fields: Optional[JsonDict] = None,
        distinct: Optional[str] = None,
        limit: int = 100,
        **kwargs: Any,
    ) -> "CatalogSearchIterator":
        """Catalog STAC search

        `Catalog API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/postSearchSTAC>`__

        :param collection: A data collection object or a collection ID
        :param time: A time interval or a single time. It can either be a string in form  YYYY-MM-DDThh:mm:ss or
            YYYY-MM-DD or a datetime object
        :param bbox: A search bounding box, it will always be reprojected to WGS 84 before being sent to the service.
            Re-projection will be done with BBox.transform_bounds method which can produce a slightly larger bounding
            box. If that is a problem then transform a BBox object into a Geometry object and search with geometry
            parameter instead.
        :param geometry: A search geometry, it will always reprojected to WGS 84 before being sent to the service.
            This parameter is defined with parameter `intersects` at the service.
        :param ids: A list of feature ids as defined in service documentation
        :param filter: A STAC filter in CQL2, described in Catalog API documentation
        :param filter_lang: How to parse CQL2 of the `filter` input, described in Catalog API documentation
        :param filter_crs: The CRS used by spatial literals in the 'filter' value, provided in URI form. Example input
            is `"http://www.opengis.net/def/crs/OGC/1.3/CRS84"`
        :param fields: A dictionary of fields to include or exclude described in Catalog API documentation
        :param distinct: A special query attribute described in Catalog API documentation
        :param limit: A number of results to return per each request. At the end iterator will always provide all
            results the difference is only in how many requests it will have to make in the background.
        :param kwargs: Any other parameters that will be passed directly to the service
        """

        if "query" in kwargs:
            raise ValueError(
                "The parameter `query` has been deprecated and replaced by `filter` in the Catalog 1.0.0 update. The"
                " queries/filters are now done in the CQL2 language."
            )

        url = f"{self.service_url}/search"

        collection_id = self._parse_collection_id(collection)
        start_time, end_time = serialize_time(parse_time_interval(time, allow_undefined=True), use_tz=True)

        if bbox and bbox.crs is not CRS.WGS84:
            bbox = bbox.transform_bounds(CRS.WGS84)
        if geometry and geometry.crs is not CRS.WGS84:
            geometry = geometry.transform(CRS.WGS84)

        payload = remove_undefined(
            {
                "collections": [collection_id],
                "datetime": f"{start_time}/{end_time}" if time else None,
                "bbox": list(bbox) if bbox else None,
                "intersects": geometry.get_geojson(with_crs=False) if geometry else None,
                "ids": ids,
                "filter": self._prepare_filters(filter, collection, filter_lang),
                "filter-lang": filter_lang,
                "filter-crs": filter_crs,
                "fields": fields,
                "distinct": distinct,
                "limit": limit,
                **kwargs,
            }
        )

        return CatalogSearchIterator(self.client, url, payload)

    @staticmethod
    def _parse_collection_id(collection: Union[str, DataCollection]) -> str:
        """Extracts catalog collection id from an object defining a collection."""
        if isinstance(collection, DataCollection):
            return collection.catalog_id
        if isinstance(collection, str):
            return collection
        raise ValueError(f"Expected either a DataCollection object or a collection id string, got {collection}")

    def _prepare_filters(
        self,
        filter_query: Union[None, str, JsonDict],
        collection: Union[DataCollection, str],
        filter_lang: Literal["cql2-text", "cql2-json"],
    ) -> Union[None, str, JsonDict]:
        """Asserts that the input coincides with the selected filter language and adds any collection filters."""
        input_missmatch_msg = f"Filter query is {filter_query} but the filter language is set to {filter_lang}."

        collection_filters = self._get_data_collection_filters(collection)

        if filter_lang == "cql2-text":
            if not (filter_query is None or isinstance(filter_query, str)):
                raise ValueError(input_missmatch_msg)

            text_queries = [f"{field}='{value}'" for field, value in collection_filters.items()]
            if filter_query:
                text_queries.append(filter_query)
            return " AND ".join(text_queries) if text_queries else None

        if filter_lang == "cql2-json":
            if not (filter_query is None or isinstance(filter_query, dict)):
                raise ValueError(input_missmatch_msg)

            json_queries = [
                {"op": "=", "args": [{"property": field}, value]} for field, value in collection_filters.items()
            ]
            if filter_query:
                json_queries.append(filter_query)
            return {"op": "and", "args": json_queries} if json_queries else None

        raise ValueError(
            f'Parameter `filter_lang` must be on of "cql2-text" or "cql2-json" but {filter_lang} was given.'
        )

    @staticmethod
    def _get_data_collection_filters(data_collection: Union[DataCollection, str]) -> Dict[str, str]:
        """Builds a `field: value` dictionary to create filters for catalog API corresponding to a data collection
        definition.
        """
        filters: Dict[str, str] = {}

        if isinstance(data_collection, str):
            return filters

        if data_collection.swath_mode:
            filters["sar:instrument_mode"] = data_collection.swath_mode.upper()

        if data_collection.polarization:
            filters["s1:polarization"] = data_collection.polarization.upper()

        if data_collection.resolution:
            filters["s1:resolution"] = data_collection.resolution.upper()

        if data_collection.orbit_direction and data_collection.orbit_direction.upper() != OrbitDirection.BOTH:
            filters["sat:orbit_state"] = data_collection.orbit_direction.lower()

        if data_collection.timeliness:
            filters["s1:timeliness"] = data_collection.timeliness

        return filters


class CatalogSearchIterator(FeatureIterator[JsonDict]):
    """Searches a catalog with a given query and provides results"""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.next: Optional[JsonDict] = None

    def _fetch_features(self) -> Iterable[JsonDict]:
        """Collects more results from the service"""
        payload = remove_undefined({**self.params, "next": self.next})

        results = self.client.get_json_dict(self.url, post_values=payload, use_session=True)

        self.next = results["context"].get("next")
        new_features = results["features"]
        self.finished = self.next is None or not new_features

        return new_features

    def get_timestamps(self) -> List[dt.datetime]:
        """Provides features timestamps

        :return: A list of sensing times
        """
        return [parse_time(feature["properties"]["datetime"], force_datetime=True) for feature in self]

    def get_geometries(self) -> List[Geometry]:
        """Provides features geometries

        :return: A list of geometry objects with CRS
        """
        return [Geometry.from_geojson(feature["geometry"]) for feature in self]

    def get_ids(self) -> List[str]:
        """Provides features IDs

        :return: A list of IDs
        """
        return [feature["id"] for feature in self]

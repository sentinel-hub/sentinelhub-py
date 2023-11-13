"""
A client interface for `Sentinel Hub Catalog API <https://docs.sentinel-hub.com/api/latest/api/catalog>`__.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Iterable, Literal

from ..base import FeatureIterator
from ..config import SHConfig
from ..data_collections import DataCollection, OrbitDirection
from ..geometry import CRS, BBox, Geometry
from ..time_utils import filter_times, parse_time, parse_time_interval, serialize_time
from ..types import JsonDict, RawTimeIntervalType, RawTimeType
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

    def get_collections(self) -> list[JsonDict]:
        """Provides a list of collections that are available to a user

        `Catalog API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getCollections>`__

        :return: A list of collections with information
        """
        return self.client.get_json_dict(f"{self.service_url}/collections", use_session=True)["collections"]

    def get_collection(self, collection: DataCollection | str) -> JsonDict:
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

    # pylint: disable=too-many-arguments
    def search(
        self,
        collection: DataCollection | str,
        *,
        time: RawTimeType | RawTimeIntervalType = None,
        bbox: BBox | None = None,
        geometry: Geometry | None = None,
        ids: list[str] | None = None,
        filter: None | str | JsonDict = None,  # pylint: disable=redefined-builtin # noqa: A002
        filter_lang: Literal["cql2-text", "cql2-json"] = "cql2-text",
        filter_crs: str | None = None,
        fields: JsonDict | None = None,
        distinct: str | None = None,
        limit: int = 100,
        **kwargs: Any,
    ) -> CatalogSearchIterator:
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

        payload = remove_undefined({
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
        })

        return CatalogSearchIterator(self.client, url, payload)

    @staticmethod
    def _parse_collection_id(collection: str | DataCollection) -> str:
        """Extracts catalog collection id from an object defining a collection."""
        if isinstance(collection, DataCollection):
            return collection.catalog_id
        if isinstance(collection, str):
            return collection
        raise ValueError(f"Expected either a DataCollection object or a collection id string, got {collection}")

    def _prepare_filters(
        self,
        filter_query: None | str | JsonDict,
        collection: DataCollection | str,
        filter_lang: Literal["cql2-text", "cql2-json"],
    ) -> None | str | JsonDict:
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
    def _get_data_collection_filters(data_collection: DataCollection | str) -> dict[str, str]:
        """Builds a `field: value` dictionary to create filters for catalog API corresponding to a data collection
        definition.
        """
        filters: dict[str, str] = {}

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
        self.next: JsonDict | None = None

    def _fetch_features(self) -> Iterable[JsonDict]:
        """Collects more results from the service"""
        payload = remove_undefined({**self.params, "next": self.next})

        results = self.client.get_json_dict(self.url, post_values=payload, use_session=True)

        self.next = results["context"].get("next")
        new_features = results["features"]
        self.finished = self.next is None or not new_features

        return new_features

    def get_timestamps(self) -> list[dt.datetime]:
        """Provides features timestamps

        :return: A list of sensing times
        """
        return [parse_time(feature["properties"]["datetime"], force_datetime=True) for feature in self]

    def get_geometries(self) -> list[Geometry]:
        """Provides features geometries

        :return: A list of geometry objects with CRS
        """
        return [Geometry.from_geojson(feature["geometry"]) for feature in self]

    def get_ids(self) -> list[str]:
        """Provides features IDs

        :return: A list of IDs
        """
        return [feature["id"] for feature in self]


def get_available_timestamps(
    bbox: BBox,
    time_interval: RawTimeIntervalType | None,
    data_collection: DataCollection,
    *,
    time_difference: dt.timedelta | None = None,
    ignore_tz: bool = True,
    maxcc: float | None = None,
    config: SHConfig | None = None,
) -> list[dt.datetime]:
    """Helper function to search for all available timestamps for a given area and query parameters.

    :param bbox: A bounding box of the search area.
    :param data_collection: A collection specifying the satellite data source for finding available timestamps.
    :param time_interval: A time interval from which to provide the timestamps.
    :param time_difference: Shortest allowed time difference. Consecutive timestamps will be skipped if too close to
        the previous one. Defaults to keeping all timestamps.
    :param ignore_tz: Ignore the time zone part in the returned timestamps. Default is True.
    :param maxcc: Maximum cloud coverage filter from interval [0, 1]. Default is None.
    :param config: The SH configuration object.
    :return: A list of timestamps of available observations.
    """
    query_filter = None
    time_difference = time_difference if time_difference is not None else dt.timedelta(seconds=-1)
    fields = {"include": ["properties.datetime"], "exclude": []}

    if maxcc is not None and data_collection.has_cloud_coverage:
        if isinstance(maxcc, (int, float)) and (maxcc < 0 or maxcc > 1):
            raise ValueError('Maximum cloud coverage "maxcc" parameter should be a float on an interval [0, 1]')
        query_filter = f"eo:cloud_cover < {int(maxcc * 100)}"

    if data_collection.service_url is not None:
        config = config.copy() if config else SHConfig()
        config.sh_base_url = data_collection.service_url

    catalog = SentinelHubCatalog(config=config)
    search_iterator = catalog.search(
        collection=data_collection, bbox=bbox, time=time_interval, filter=query_filter, fields=fields
    )

    timestamps = [parse_time(ts, force_datetime=True, ignoretz=ignore_tz) for ts in search_iterator.get_timestamps()]
    return filter_times(timestamps, time_difference)

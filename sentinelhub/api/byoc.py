"""
Module implementing an interface with
`Sentinel Hub Bring Your Own COG API <https://docs.sentinel-hub.com/api/latest/api/byoc/>`__.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, Union

from dataclasses_json import CatchAll, LetterCase, Undefined
from dataclasses_json import config as dataclass_config
from dataclasses_json import dataclass_json

from ..constants import MimeType, RequestType
from ..data_collections import DataCollection
from ..geometry import Geometry
from ..types import Json, JsonDict
from .base import BaseCollection, SentinelHubFeatureIterator, SentinelHubService
from .utils import datetime_config, geometry_config, remove_undefined

CollectionType = Union["ByocCollection", DataCollection, dict, str]
TileType = Union["ByocTile", dict, str]


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass
class ByocCollectionBand:
    """Dataclass to hold BYOC collection band specification"""

    source: Optional[str] = None
    band_index: Optional[int] = None
    bit_depth: int = 8
    sample_format: str = "UINT"
    no_data: Optional[float] = None
    other_data: CatchAll = field(default_factory=dict)


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass
class ByocCollectionAdditionalData:
    """Dataclass to hold BYOC collection additional data"""

    bands: Optional[Dict[str, ByocCollectionBand]] = None
    max_meters_per_pixel: Optional[float] = None
    max_meters_per_pixel_override: Optional[float] = None
    other_data: CatchAll = field(default_factory=dict)


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass
class ByocCollection(BaseCollection):
    """Dataclass to hold BYOC collection data"""

    additional_data: Optional[ByocCollectionAdditionalData] = None


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass
class ByocTile:
    """Dataclass to hold BYOC tile data"""

    path: str
    status: Optional[str] = None
    tile_id: Optional[str] = field(metadata=dataclass_config(field_name="id"), default=None)
    tile_geometry: Optional[Geometry] = field(metadata=geometry_config, default=None)
    cover_geometry: Optional[Geometry] = field(metadata=geometry_config, default=None)
    created: Optional[datetime] = field(metadata=datetime_config, default=None)
    sensing_time: Optional[datetime] = field(metadata=datetime_config, default=None)
    additional_data: Optional[dict] = None
    other_data: CatchAll = field(default_factory=dict)


class SentinelHubBYOC(SentinelHubService):
    """An interface class for Sentinel Hub Bring your own COG (BYOC) API

    For more info check `BYOC API reference
    <https://docs.sentinel-hub.com/api/latest/reference/#tag/byoc_collection>`__.
    """

    @staticmethod
    def _get_service_url(base_url: str) -> str:
        """Provides URL to Catalog API"""
        return f"{base_url}/api/v1/byoc"

    def iter_collections(self, search: Optional[str] = None, **kwargs: Any) -> SentinelHubFeatureIterator:
        """Retrieve collections

        `BYOC API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getByocCollections>`__

        :param search: A search query
        :param kwargs: Any other request parameters
        :return: iterator over collections
        """
        return SentinelHubFeatureIterator(
            client=self.client,
            url=f"{self.service_url}/collections",
            params={"search": search, **kwargs},
            exception_message="Failed to obtain information about available BYOC collections",
        )

    def get_collection(self, collection: CollectionType) -> JsonDict:
        """Get collection by its id

        `BYOC API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getByocCollectionById>`__

        :param collection: a ByocCollection, dict or collection id string
        :return: dictionary of the collection
        """
        url = f"{self.service_url}/collections/{self._parse_id(collection)}"
        return self.client.get_json_dict(url=url, use_session=True, extract_key="data")

    def create_collection(self, collection: CollectionType) -> JsonDict:
        """Create a new collection

        `BYOC API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/createByocCollection>`__

        :param collection: ByocCollection object or a dictionary
        :return: dictionary of the created collection
        """
        coll = self._to_dict(collection)
        url = f"{self.service_url}/collections"
        return self.client.get_json_dict(url=url, post_values=coll, use_session=True, extract_key="data")

    def update_collection(self, collection: CollectionType) -> Json:
        """Update an existing collection

        `BYOC API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/updateByocCollectionById>`__

        :param collection: ByocCollection object or a dictionary
        """
        coll = self._to_dict(collection)
        url = f"{self.service_url}/collections/{self._parse_id(coll)}"
        headers = {"Content-Type": MimeType.JSON.get_string()}
        return self.client.get_json(
            url=url, request_type=RequestType.PUT, post_values=coll, headers=headers, use_session=True
        )

    def delete_collection(self, collection: CollectionType) -> Json:
        """Delete existing collection by its id

        `BYOC API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/deleteByocCollectionById>`__

        :param collection: a ByocCollection, dict or collection id string
        """
        url = f"{self.service_url}/collections/{self._parse_id(collection)}"
        return self.client.get_json(url=url, request_type=RequestType.DELETE, use_session=True)

    def copy_tiles(self, from_collection: CollectionType, to_collection: CollectionType) -> Json:
        """Copy tiles from one collection to another

        `BYOC API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/copyByocCollectionTiles>`__

        :param from_collection: a ByocCollection, dict or collection id string
        :param to_collection: a ByocCollection, dict or collection id string
        """
        url = (
            f"{self.service_url}/collections/{self._parse_id(from_collection)}"
            f"/copyTiles?toCollection={self._parse_id(to_collection)}"
        )
        return self.client.get_json(url=url, request_type=RequestType.POST, use_session=True)

    def iter_tiles(
        self, collection: CollectionType, sort: Optional[str] = None, path: Optional[str] = None, **kwargs: Any
    ) -> SentinelHubFeatureIterator:
        """Iterator over collection tiles

        `BYOC API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getByocCollectionTiles>`__

        :param collection: a ByocCollection, dict or collection id string
        :param sort: Order in which to return tiles
        :param path: An exact path where tiles are located
        :param kwargs: Any other request parameters
        :return: An iterator over payloads of tiles from the collection
        """
        collection_id = self._parse_id(collection)
        return SentinelHubFeatureIterator(
            client=self.client,
            url=f"{self.service_url}/collections/{collection_id}/tiles",
            params={"sort": sort, "path": path, **kwargs},
            exception_message=f"Failed to obtain information about tiles in BYOC collection {collection_id}",
        )

    def get_tile(self, collection: CollectionType, tile: TileType) -> JsonDict:
        """Get a tile of collection

        `BYOC API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getByocCollectionTileById>`__

        :param collection: a ByocCollection, dict or collection id string
        :param tile: a ByocTile, dict or tile id string
        :return: dictionary of the tile
        """
        url = f"{self.service_url}/collections/{self._parse_id(collection)}/tiles/{self._parse_id(tile)}"
        return self.client.get_json_dict(url=url, use_session=True, extract_key="data")

    def create_tile(self, collection: CollectionType, tile: TileType) -> JsonDict:
        """Create tile within collection

        `BYOC API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/createByocCollectionTile>`__

        :param collection: a ByocCollection, dict or collection id string
        :param tile: a ByocTile or dict
        :return: dictionary of the tile
        """
        _tile = self._to_dict(tile)
        url = f"{self.service_url}/collections/{self._parse_id(collection)}/tiles"
        return self.client.get_json_dict(url=url, post_values=_tile, use_session=True, extract_key="data")

    def update_tile(self, collection: CollectionType, tile: TileType) -> Json:
        """Update a tile within collection

        `BYOC API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/updateByocCollectionTileById>`__

        :param collection: a ByocCollection, dict or collection id string
        :param tile: a ByocTile or dict
        """
        url = f"{self.service_url}/collections/{self._parse_id(collection)}/tiles/{self._parse_id(tile)}"
        headers = {"Content-Type": MimeType.JSON.get_string()}

        _tile = self._to_dict(tile)
        updates = remove_undefined(
            {
                "path": _tile["path"],
                "coverGeometry": _tile.get("coverGeometry"),
                "sensingTime": _tile.get("sensingTime"),
            }
        )

        return self.client.get_json(
            url=url, request_type=RequestType.PUT, post_values=updates, headers=headers, use_session=True
        )

    def delete_tile(self, collection: CollectionType, tile: TileType) -> Json:
        """Delete a tile from collection

        `BYOC API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/deleteByocCollectionTileById>`__

        :param collection: a ByocCollection, dict or collection id string
        :param tile: a ByocTile, dict or tile id string
        """
        url = f"{self.service_url}/collections/{self._parse_id(collection)}/tiles/{self._parse_id(tile)}"
        return self.client.get_json(url=url, request_type=RequestType.DELETE, use_session=True)

    def reingest_tile(self, collection: CollectionType, tile: TileType) -> Json:
        """Re-ingests a tile into a collection

        `BYOC API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/reingestByocCollectionTileById>`__

        :param collection: a ByocCollection, dict or collection id string
        :param tile: a ByocTile, dict or tile id string
        """
        url = f"{self.service_url}/collections/{self._parse_id(collection)}/tiles/{self._parse_id(tile)}/reingest"
        return self.client.get_json(url=url, request_type=RequestType.POST, use_session=True)

    @staticmethod
    def _parse_id(data: object) -> Optional[str]:
        if isinstance(data, (ByocCollection, DataCollection)):
            return data.collection_id
        if isinstance(data, ByocTile):
            return data.tile_id
        if isinstance(data, dict):
            return data["id"]
        if isinstance(data, str):
            return data
        raise ValueError(f"Expected a BYOC/Data dataclass, dictionary or a string, got {data}.")

    @staticmethod
    def _to_dict(data: object) -> dict:
        """Constructs dict from an object (either dataclass or dict)"""
        if isinstance(data, (ByocCollection, ByocTile, ByocCollectionAdditionalData, ByocCollectionBand)):
            return data.to_dict()  # type: ignore[union-attr] # to_dict method comes from decorators and is undetectable
        if isinstance(data, dict):
            return data
        raise ValueError(f"Expected either a data class (e.g., ByocCollection and similar) or a dict, got {data}.")

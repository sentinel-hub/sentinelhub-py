"""
Interface of
`Sentinel Hub Web Feature Service (WFS) <https://www.sentinel-hub.com/develop/api/ogc/standard-parameters/wfs/>`__.
"""

from __future__ import annotations

import datetime as dt
from typing import Iterable
from urllib.parse import urlencode

import shapely.geometry

from ..base import FeatureIterator
from ..config import SHConfig
from ..constants import CRS, MimeType, ServiceType, SHConstants
from ..data_collections import DataCollection
from ..download import SentinelHubDownloadClient
from ..geometry import BBox
from ..time_utils import parse_time, parse_time_interval, serialize_time
from ..types import JsonDict, RawTimeIntervalType, RawTimeType


class WebFeatureService(FeatureIterator[JsonDict]):
    """Class for interaction with Sentinel Hub WFS service

    The class is an iterator over info about all available satellite tiles for requested parameters. It collects data
    from Sentinel Hub service only during the first iteration. During next iterations it returns already obtained data.
    The data is in the same order as returned by the service.

    For more info check `WFS documentation <https://www.sentinel-hub.com/develop/api/ogc/standard-parameters/wfs/>`__.
    """

    def __init__(
        self,
        bbox: BBox,
        time_interval: RawTimeType | RawTimeIntervalType,
        *,
        data_collection: DataCollection,
        maxcc: float = 1.0,
        config: SHConfig | None = None,
    ):
        """
        :param bbox: Bounding box of the requested image. Coordinates must be in the specified coordinate reference
            system.
        :param time_interval: interval with start and end date of the form YYYY-MM-DDThh:mm:ss or YYYY-MM-DD
        :param data_collection: A collection of requested satellite data
        :param maxcc: Maximum accepted cloud coverage of an image. Float between 0.0 and 1.0. Default is 1.0.
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        self.config = config or SHConfig()
        if not self.config.instance_id:
            raise ValueError(
                "Sentinel Hub instance ID should be provided with SHConfig or saved into the configuration file."
                "Check https://sentinelhub-py.readthedocs.io/en/latest/configure.html for more info."
            )

        self.bbox = bbox

        self.latest_time_only = time_interval == SHConstants.LATEST
        if not self.latest_time_only:
            self.time_interval = parse_time_interval(time_interval)
        else:
            self.time_interval = dt.datetime(year=1985, month=1, day=1), dt.datetime.now()

        self.data_collection = data_collection
        self.maxcc = maxcc
        self.max_features_per_request = 1 if self.latest_time_only else self.config.max_wfs_records_per_query

        client = SentinelHubDownloadClient(config=self.config)
        url = self._build_service_url()
        params = self._build_request_params()

        super().__init__(client, url, params)
        self.next: int = 0

    def _build_service_url(self) -> str:
        """Creates a base URL for WFS service"""
        base_url = f"{self.config.sh_base_url}/ogc"

        if self.data_collection.service_url:
            base_url = base_url.replace(self.config.sh_base_url, self.data_collection.service_url)

        return f"{base_url}/{ServiceType.WFS.value}/{self.config.instance_id}"

    def _build_request_params(self) -> JsonDict:
        """Builds URL parameters for WFS service"""
        start_time, end_time = serialize_time(self.time_interval, use_tz=True)
        bbox = self.bbox.reverse() if self.bbox.crs is CRS.WGS84 else self.bbox
        return {
            "SERVICE": ServiceType.WFS.value,
            "WARNINGS": False,
            "REQUEST": "GetFeature",
            "TYPENAMES": self.data_collection.wfs_id,
            "BBOX": ",".join(map(str, bbox)),
            "OUTPUTFORMAT": MimeType.JSON.get_string(),
            "SRSNAME": self.bbox.crs.ogc_string(),
            "TIME": f"{start_time}/{end_time}",
            "MAXCC": 100.0 * self.maxcc,
            "MAXFEATURES": self.max_features_per_request,
        }

    def _fetch_features(self) -> Iterable[JsonDict]:
        """Collects data from WFS service"""
        params: JsonDict = {**self.params, "FEATURE_OFFSET": self.next}
        url = f"{self.url}?{urlencode(params)}"

        new_features = self.client.get_json_dict(url)["features"]

        if len(new_features) < self.max_features_per_request or self.latest_time_only:
            self.finished = True
        else:
            self.next += self.max_features_per_request

        is_sentinel1 = self.data_collection.is_sentinel1
        return [
            feature_info
            for feature_info in new_features
            if not is_sentinel1 or self._sentinel1_product_check(feature_info)
        ]

    def get_dates(self) -> list[dt.date | None]:
        """Returns a list of acquisition times from tile info data

        :return: List of acquisition times in the order returned by WFS service.
        """
        tile_dates: list[dt.date | None] = []

        for tile_info in self:
            if not tile_info["properties"]["date"]:  # could be True for custom (BYOC) data collections
                tile_dates.append(None)
            else:
                date_str = tile_info["properties"]["date"]
                time_str = tile_info["properties"]["time"]
                tile_dates.append(parse_time(f"{date_str}T{time_str}"))

        return tile_dates

    def get_geometries(self) -> list[shapely.geometry.MultiPolygon]:
        """Returns a list of geometries from tile info data

        :return: List of multipolygon geometries in the order returned by WFS service.
        """
        return [shapely.geometry.shape(tile_info["geometry"]) for tile_info in self]

    def get_tiles(self) -> list[tuple[str, str, int]]:
        """Returns list of tiles with tile name, date and AWS index

        :return: List of tiles in form of (tile_name, date, aws_index)
        """
        return [self._parse_tile_url(tile_info["properties"]["path"]) for tile_info in self]

    @staticmethod
    def _parse_tile_url(tile_url: str) -> tuple[str, str, int]:
        """Extracts tile name, data and AWS index from tile URL

        :param tile_url: Location of tile at AWS
        :return: Tuple in a form (tile_name, date, aws_index)
        """
        props = tile_url.rsplit("/", 7)
        return "".join(props[1:4]), "-".join(props[4:7]), int(props[7])

    def _sentinel1_product_check(self, tile_info: JsonDict) -> bool:
        """Checks if Sentinel-1 tile info match the data collection definition"""
        product_id = tile_info["properties"]["id"]
        props = product_id.split("_")
        swath_mode, resolution, polarization = props[1], props[2][3], props[3][2:4]
        orbit_direction = tile_info["properties"].get("orbitDirection", "")

        if not (swath_mode in ["IW", "EW"] and resolution in ["M", "H"] and polarization in ["DV", "DH", "SV", "SH"]):
            raise ValueError(f"Unknown Sentinel-1 tile type: {product_id}")

        return (
            (swath_mode == self.data_collection.swath_mode or self.data_collection.swath_mode is None)
            and (polarization == self.data_collection.polarization or self.data_collection.polarization is None)
            and (resolution == self.data_collection.resolution[0] or self.data_collection.resolution is None)
            and self.data_collection.contains_orbit_direction(orbit_direction)
        )

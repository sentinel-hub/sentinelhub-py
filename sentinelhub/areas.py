"""
Module for working with large geographical areas
"""
import itertools
import json
import math
import os
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Tuple, TypeVar, Union, cast

import shapely
import shapely.geometry
import shapely.ops
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry

from .api import BatchRequest, SentinelHubBatch, SentinelHubCatalog
from .config import SHConfig
from .constants import CRS
from .data_collections import DataCollection
from .geo_utils import transform_point
from .geometry import BBox, BBoxCollection, Geometry, _BaseGeometry
from .types import JsonDict

T = TypeVar("T", float, int)


class AreaSplitter(metaclass=ABCMeta):
    """Abstract class for splitter classes. It implements common methods used for splitting large area into smaller
    parts.
    """

    def __init__(
        self,
        shape_list: Iterable[Union[Polygon, MultiPolygon, _BaseGeometry]],
        crs: CRS,
        reduce_bbox_sizes: bool = False,
    ):
        """
        :param shape_list: A list of geometrical shapes describing the area of interest
        :param crs: Coordinate reference system of the shapes in `shape_list`
        :param reduce_bbox_sizes: If `True` it will reduce the sizes of bounding boxes so that they will tightly fit
            the given geometry in `shape_list`.
        """
        self.crs = CRS(crs)
        self.shape_list = [self._parse_shape(shape, crs) for shape in shape_list]
        self.area_shape = self._join_shape_list(self.shape_list)
        self.reduce_bbox_sizes = reduce_bbox_sizes

        self.area_bbox = self.get_area_bbox()
        self.bbox_list, self.info_list = self._make_split()

    @staticmethod
    def _parse_shape(shape: Union[Polygon, MultiPolygon, _BaseGeometry], crs: CRS) -> Union[Polygon, MultiPolygon]:
        """Helper method for parsing input shapes"""
        if isinstance(shape, (Polygon, MultiPolygon)):
            return shape
        if isinstance(shape, _BaseGeometry):
            return shape.transform(crs).geometry
        raise ValueError(
            f"The list of shapes must contain shapes of types {Polygon}, {MultiPolygon} or subtype of {_BaseGeometry}"
        )

    @staticmethod
    def _join_shape_list(shape_list: List[Union[Polygon, MultiPolygon]]) -> MultiPolygon:
        """Joins a list of shapes together into one shape

        :param shape_list: A list of geometrical shapes describing the area of interest
        :return: A multipolygon which is a union of shapes in given list
        """
        if shapely.__version__ >= "1.8.0":
            return shapely.ops.unary_union(shape_list)
        return shapely.ops.cascaded_union(shape_list)

    @abstractmethod
    def _make_split(self) -> Tuple[List[BBox], List[Dict[str, object]]]:
        """The abstract method where the splitting will happen"""

    def get_bbox_list(
        self,
        crs: Optional[CRS] = None,
        buffer: Union[None, float, Tuple[float, float]] = None,
        reduce_bbox_sizes: Optional[bool] = None,
    ) -> List[BBox]:
        """Returns a list of bounding boxes that are the result of the split

        :param crs: Coordinate reference system in which the bounding boxes should be returned. If `None` the CRS will
            be the default CRS of the splitter.
        :param buffer: A percentage of each BBox size increase. This will cause neighbouring bounding boxes to overlap.
        :param reduce_bbox_sizes: If `True` it will reduce the sizes of bounding boxes so that they will tightly
            fit the given geometry in `shape_list`. This overrides the same parameter from constructor
        :return: List of bounding boxes
        """
        bbox_list = self.bbox_list
        if buffer:
            bbox_list = [bbox.buffer(buffer, relative=True) for bbox in bbox_list]

        if reduce_bbox_sizes is None:
            reduce_bbox_sizes = self.reduce_bbox_sizes
        if reduce_bbox_sizes:
            bbox_list = self._reduce_sizes(bbox_list)

        if crs:
            return [bbox.transform(crs) for bbox in bbox_list]
        return bbox_list

    def get_geometry_list(self) -> List[Union[Polygon, MultiPolygon]]:
        """For each bounding box an intersection with the shape of entire given area is calculated. CRS of the returned
        shapes is the same as CRS of the given area.

        :return: List of polygons or multipolygons corresponding to the order of bounding boxes
        """
        return [self._intersection_area(bbox) for bbox in self.bbox_list]

    def get_info_list(self) -> List[Dict[str, object]]:
        """Returns a list of dictionaries containing information about bounding boxes obtained in split. The order in
        the list matches the order of the list of bounding boxes.

        :return: List of dictionaries
        """
        return self.info_list

    def get_area_shape(self) -> MultiPolygon:
        """Returns a single shape of entire area described with `shape_list` parameter

        :return: A multipolygon which is a union of shapes describing the area
        """
        return self.area_shape

    def get_area_bbox(self, crs: Optional[CRS] = None) -> BBox:
        """Returns a bounding box of the entire area

        :param crs: Coordinate reference system in which the bounding box should be returned. If `None` the CRS will
            be the default CRS of the splitter.
        :return: A bounding box of the area defined by the `shape_list`
        """
        bbox_list = [BBox(shape.bounds, crs=self.crs) for shape in self.shape_list]
        area_min_x = min(bbox.lower_left[0] for bbox in bbox_list)
        area_min_y = min(bbox.lower_left[1] for bbox in bbox_list)
        area_max_x = max(bbox.upper_right[0] for bbox in bbox_list)
        area_max_y = max(bbox.upper_right[1] for bbox in bbox_list)
        bbox = BBox([area_min_x, area_min_y, area_max_x, area_max_y], crs=self.crs)
        if crs is None:
            return bbox
        return bbox.transform(crs)

    def _intersects_area(self, bbox: BBox) -> bool:
        """Checks if the bounding box intersects the entire area

        :param bbox: A bounding box
        :return: `True` if bbox intersects the entire area else False
        """
        return self._bbox_to_area_polygon(bbox).intersects(self.area_shape)

    def _intersection_area(self, bbox: BBox) -> Union[Polygon, MultiPolygon]:
        """Calculates the intersection of a given bounding box and the entire area

        :param bbox: A bounding box
        :return: A shape of intersection
        """
        return self._bbox_to_area_polygon(bbox).intersection(self.area_shape)

    def _bbox_to_area_polygon(self, bbox: BBox) -> Polygon:
        """Transforms bounding box into a polygon object in the area CRS.

        :param bbox: A bounding box
        :return: A polygon
        """
        projected_bbox = bbox.transform(self.crs)
        return projected_bbox.geometry

    def _reduce_sizes(self, bbox_list: List[BBox]) -> List[BBox]:
        """Reduces sizes of bounding boxes"""
        return [BBox(self._intersection_area(bbox).bounds, self.crs).transform(bbox.crs) for bbox in bbox_list]


class BBoxSplitter(AreaSplitter):
    """A tool that splits the given area into smaller parts. Given the area it calculates its bounding box and splits
    it into smaller bounding boxes of equal size. Then it filters out the bounding boxes that do not intersect the
    area. If specified by user it can also reduce the sizes of the remaining bounding boxes to best fit the area.
    """

    def __init__(
        self,
        shape_list: Iterable[Union[Polygon, MultiPolygon, _BaseGeometry]],
        crs: CRS,
        split_shape: Union[None, int, Tuple[int, int]] = None,
        split_size: Union[None, int, Tuple[int, int]] = None,
        **kwargs: Any,
    ):
        """
        :param shape_list: A list of geometrical shapes describing the area of interest
        :param crs: Coordinate reference system of the shapes in `shape_list`
        :param split_shape: Parameter that describes the shape in which the area bounding box will be split.
            It can be a tuple of the form `(n, m)` which means the area bounding box will be split into `n` columns
            and `m` rows. It can also be a single integer `n` which is the same as `(n, n)`.
        :param split_size: Parameter that describes the size of patches (in the same Unit of Measure of the CRS)
         into which the area bounding box will be split.
            It can be a tuple of the form `(width, height)` which means the area bounding box will be split into patches
            of size (width, height). It can also be a single integer `size` which is the same as `(size, size)`.
        :param reduce_bbox_sizes: If `True` it will reduce the sizes of bounding boxes so that they will tightly fit
            the given area geometry from `shape_list`.
        """
        if (split_shape is not None) and (split_size is None):
            self.split_params = ("shape", _parse_to_pair(split_shape, allowed_types=(int,), param_name="split_shape"))
        elif (split_shape is None) and (split_size is not None):
            self.split_params = ("size", _parse_to_pair(split_size, allowed_types=(int,), param_name="split_size"))
        else:
            raise ValueError("Exactly one of 'split_shape' or 'split_size' needs to be specified.")
        super().__init__(shape_list, crs, **kwargs)

    def _make_split(self) -> Tuple[List[BBox], List[Dict[str, object]]]:
        mode, split_params = self.split_params
        if mode == "shape":
            columns, rows = split_params
            bbox_partition = self.area_bbox.get_partition(num_x=columns, num_y=rows)
        else:
            width, height = split_params
            bbox_partition = self.area_bbox.get_partition(size_x=width, size_y=height)

            columns = len(bbox_partition)
            rows = len(bbox_partition[0])

        bbox_list, info_list = [], []
        for i, j in itertools.product(range(columns), range(rows)):
            if self._intersects_area(bbox_partition[i][j]):
                bbox_list.append(bbox_partition[i][j])

                info = {"parent_bbox": self.area_bbox, "index_x": i, "index_y": j}
                info_list.append(info)

        return bbox_list, info_list


class OsmSplitter(AreaSplitter):
    """A tool that splits the given area into smaller parts. For the splitting it uses Open Street Map (OSM) grid on
    the specified zoom level. It calculates bounding boxes of all OSM tiles that intersect the area. If specified by
    user it can also reduce the sizes of the remaining bounding boxes to best fit the area.
    """

    def __init__(
        self,
        shape_list: Iterable[Union[Polygon, MultiPolygon, _BaseGeometry]],
        crs: CRS,
        zoom_level: int,
        **kwargs: Any,
    ):
        """
        :param shape_list: A list of geometrical shapes describing the area of interest
        :param crs: Coordinate reference system of the shapes in `shape_list`
        :param zoom_level: A zoom level defined by OSM. Level 0 is entire world, level 1 splits the world into
            4 parts, etc.
        :param reduce_bbox_sizes: If `True` it will reduce the sizes of bounding boxes so that they will tightly fit
            the given area geometry from `shape_list`.
        """
        self._POP_WEB_MAX = transform_point((180, 0), CRS.WGS84, CRS.POP_WEB)[0]  # pylint: disable=invalid-name

        self.zoom_level = zoom_level
        super().__init__(shape_list, crs, **kwargs)

    def _make_split(self) -> Tuple[List[BBox], List[Dict[str, object]]]:
        self.area_bbox = self.get_area_bbox(CRS.POP_WEB)
        self._check_area_bbox()

        bbox_list, info_list = self._recursive_split(self.get_world_bbox(), 0, 0, 0)

        for i, bbox in enumerate(bbox_list):
            bbox_list[i] = bbox.transform(self.crs)
        return bbox_list, info_list

    def _check_area_bbox(self) -> None:
        """The method checks if the area bounding box is completely inside the OSM grid. That means that its latitudes
        must be contained in the interval (-85.0511, 85.0511)

        :raises: ValueError
        """
        for coord in self.area_bbox:
            if abs(coord) > self._POP_WEB_MAX:
                raise ValueError(
                    "OsmTileSplitter only works for areas which have latitude in interval (-85.0511, 85.0511)"
                )

    def get_world_bbox(self) -> BBox:
        """Creates a bounding box of the entire world in EPSG: 3857

        :return: Bounding box of entire world
        """
        return BBox((-self._POP_WEB_MAX, -self._POP_WEB_MAX, self._POP_WEB_MAX, self._POP_WEB_MAX), crs=CRS.POP_WEB)

    def _recursive_split(
        self,
        bbox: BBox,
        zoom_level: int,
        column: int,
        row: int,
    ) -> Tuple[List[BBox], List[Dict[str, object]]]:
        """Method that recursively creates bounding boxes of OSM grid that intersect the area.

        :param bbox: Bounding box
        :param zoom_level: OSM zoom level
        :param column: Column in the OSM grid
        :param row: Row in the OSM grid
        """
        if zoom_level == self.zoom_level:
            return [bbox], [{"zoom_level": zoom_level, "index_x": column, "index_y": row}]

        bbox_list, info_list = [], []
        bbox_partition = bbox.get_partition(num_x=2, num_y=2)
        for i, j in itertools.product(range(2), range(2)):
            if self._intersects_area(bbox_partition[i][j]):
                bboxes, infos = self._recursive_split(
                    bbox_partition[i][j], zoom_level + 1, 2 * column + i, 2 * row + 1 - j
                )
                bbox_list.extend(bboxes)
                info_list.extend(infos)

        return bbox_list, info_list


class TileSplitter(AreaSplitter):
    """A splitter that uses Sentinel Hub Catalog API to obtain geometries of the original tiling grid of a given
    data collection. Additionally, it can further split these geometries into smaller parts.
    """

    _CATALOG_FILTER = {
        "include": ["id", "geometry", "properties.datetime", "properties.proj:bbox", "properties.proj:epsg"],
        "exclude": [],
    }

    def __init__(
        self,
        shape_list: Iterable[Union[Polygon, MultiPolygon, _BaseGeometry]],
        crs: CRS,
        time_interval: Tuple[str, str],
        data_collection: DataCollection,
        tile_split_shape: Union[int, Tuple[int, int]] = 1,
        config: Optional[SHConfig] = None,
        **kwargs: Any,
    ):
        """
        :param shape_list: A list of geometrical shapes describing the area of interest
        :param crs: Coordinate reference system of the shapes in `shape_list`
        :param time_interval: Interval with start and end date of the form YYYY-MM-DDThh:mm:ss or YYYY-MM-DD
        :param data_collection: A satellite data collection
        :param tile_split_shape: Parameter that describes the shape in which the satellite tile bounding boxes will be
            split. It can be a tuple of the form `(n, m)` which means the tile bounding boxes will be
            split into `n` columns and `m` rows. It can also be a single integer `n` which is the same
            as `(n, n)`.
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :param kwargs: Parameters that are propagated to the base `AreaSplitter` class
        """
        self.time_interval = time_interval
        self.tile_split_shape = tile_split_shape
        self.data_collection = data_collection

        sh_config = config or SHConfig()
        if data_collection.service_url:
            sh_config = sh_config.copy()
            sh_config.sh_base_url = data_collection.service_url
        self.catalog = SentinelHubCatalog(config=sh_config)
        super().__init__(shape_list, crs, **kwargs)

    def _make_split(self) -> Tuple[List[BBox], List[Dict[str, object]]]:
        tile_dict: Dict[Tuple[Tuple[float, ...], int], Dict[str, Any]] = {}

        search_iterator = self.catalog.search(
            self.data_collection, time=self.time_interval, bbox=self.area_bbox, fields=self._CATALOG_FILTER
        )

        timestamps = search_iterator.get_timestamps()
        geometry_list = search_iterator.get_geometries()
        for tile_info, (timestamp, geometry) in zip(search_iterator, zip(timestamps, geometry_list)):
            tile_properties = tile_info["properties"]
            bbox = BBox(tile_properties["proj:bbox"], crs=tile_properties["proj:epsg"])
            bbox_hash = tuple(bbox), bbox.crs.epsg

            if bbox_hash not in tile_dict:
                tile_dict[bbox_hash] = {"bbox": bbox, "timestamps": [], "ids": [], "geometries": []}
            tile_dict[bbox_hash]["timestamps"].append(timestamp)
            tile_dict[bbox_hash]["ids"].append(tile_info["id"])
            tile_dict[bbox_hash]["geometries"].append(geometry)

        bbox_list, info_list = [], []

        for tile_info in tile_dict.values():
            if not self._intersects_area(tile_info["bbox"]):
                continue

            tile_bbox: BBox = tile_info["bbox"]
            bbox_splitter = BBoxSplitter([tile_bbox.geometry], tile_bbox.crs, split_shape=self.tile_split_shape)

            for bbox, info in zip(bbox_splitter.get_bbox_list(), bbox_splitter.get_info_list()):
                if self._intersects_area(bbox):
                    bbox_list.append(bbox)

                    info["ids"] = tile_info["ids"]
                    info["timestamps"] = tile_info["timestamps"]
                    info_list.append(info)

        return bbox_list, info_list


class CustomGridSplitter(AreaSplitter):
    """Splitting class which can split according to given custom collection of bounding boxes"""

    def __init__(
        self,
        shape_list: Iterable[Union[Polygon, MultiPolygon, _BaseGeometry]],
        crs: CRS,
        bbox_grid: Union[List[BBox], BBoxCollection],
        bbox_split_shape: Union[int, Tuple[int, int]] = 1,
        **kwargs: Any,
    ):
        """
        :param shape_list: A list of geometrical shapes describing the area of interest
        :param crs: Coordinate reference system of the shapes in `shape_list`
        :param bbox_grid: A collection of bounding boxes defining a grid of splitting. All of them have to be in the
            same CRS.
        :param bbox_split_shape: Parameter that describes the shape in which each of the bounding boxes in the given
            grid will be split. It can be a tuple of the form `(n, m)` which means the tile bounding boxes will be
            split into `n` columns and `m` rows. It can also be a single integer `n` which is the same as `(n, n)`.
        :param reduce_bbox_sizes: If `True` it will reduce the sizes of bounding boxes so that they will tightly fit
            the given geometry in `shape_list`.
        """
        self.bbox_grid = self._parse_bbox_grid(bbox_grid)
        self.bbox_split_shape = bbox_split_shape
        super().__init__(shape_list, crs, **kwargs)

    @staticmethod
    def _parse_bbox_grid(bbox_grid: Union[List[BBox], BBoxCollection]) -> BBoxCollection:
        """Helper method for parsing bounding box grid. It will try to parse it into `BBoxCollection`"""
        if isinstance(bbox_grid, BBoxCollection):
            return bbox_grid

        if isinstance(bbox_grid, list):
            return BBoxCollection(bbox_grid)

        raise ValueError(f"Parameter 'bbox_grid' should be an instance of {BBoxCollection}")

    def _make_split(self) -> Tuple[List[BBox], List[Dict[str, object]]]:
        bbox_list: List[BBox] = []
        info_list: List[Dict[str, object]] = []

        for grid_idx, grid_bbox in enumerate(self.bbox_grid):
            if self._intersects_area(grid_bbox):
                bbox_splitter = BBoxSplitter([grid_bbox.geometry], grid_bbox.crs, split_shape=self.bbox_split_shape)

                for bbox, info in zip(bbox_splitter.get_bbox_list(), bbox_splitter.get_info_list()):
                    if self._intersects_area(bbox):
                        info["grid_index"] = grid_idx

                        bbox_list.append(bbox)
                        info_list.append(info)

        return bbox_list, info_list


class BaseUtmSplitter(AreaSplitter, metaclass=ABCMeta):
    """Base splitter that returns bboxes of fixed size aligned to UTM zones or UTM grid tiles as defined by the MGRS

    The generated bounding box grid will have coordinates in form of
    `(N * bbox_size_x + offset_x, M * bbox_size_y + offset_y)`
    """

    def __init__(
        self,
        shape_list: Iterable[Union[Polygon, MultiPolygon, _BaseGeometry]],
        crs: CRS,
        bbox_size: Union[float, Tuple[float, float]],
        offset: Optional[Tuple[float, float]] = None,
    ):
        """
        :param shape_list: A list of geometrical shapes describing the area of interest
        :param crs: Coordinate reference system of the shapes in `shape_list`
        :param bbox_size: A size of generated bounding boxes in horizontal and vertical directions in meters. If a
            single value is given that will be interpreted as (value, value).
        :param offset: Bounding box offset in horizontal and vertical directions in meters.
        """
        self.bbox_size = _parse_to_pair(bbox_size, allowed_types=(int, float), param_name="bbox_size")

        self.offset = _parse_to_pair(offset or 0.0, allowed_types=(int, float), param_name="offset")

        self.utm_grid = self._get_utm_polygons()
        super().__init__(shape_list, crs)

    @abstractmethod
    def _get_utm_polygons(self) -> List[Tuple[BaseGeometry, Dict[str, Any]]]:
        """Find UTM grid zones overlapping with input area shape."""

    @staticmethod
    def _get_utm_from_props(utm_dict: Dict[str, Any]) -> CRS:
        """Return the UTM CRS corresponding to the UTM described by the properties dictionary

        :param utm_dict: Dictionary reporting name of the UTM zone and MGRS grid
        :return: UTM coordinate reference system
        """
        hemisphere_digit = 6 if utm_dict["direction"] == "N" else 7
        zone_number = utm_dict["zone"]
        return CRS(f"32{hemisphere_digit}{zone_number:02d}")

    def _align_bbox_to_size(self, bbox: BBox) -> BBox:
        """Align input bbox coordinates to be multiples of the bbox size

        :param bbox: Bounding box in UTM coordinates
        :return: BBox objects with coordinates multiples of the bbox size
        """
        size_x, size_y = self.bbox_size
        offset_x, offset_y = self.offset
        lower_left_x, lower_left_y = bbox.lower_left

        aligned_x = math.floor((lower_left_x - offset_x) / size_x) * size_x + offset_x
        aligned_y = math.floor((lower_left_y - offset_y) / size_y) * size_y + offset_y

        return BBox(((aligned_x, aligned_y), bbox.upper_right), crs=bbox.crs)

    def _make_split(self) -> Tuple[List[BBox], List[Dict[str, object]]]:
        """Split each UTM grid into equally sized bboxes in correct UTM zone"""
        size_x, size_y = self.bbox_size
        bbox_list: List[BBox] = []
        info_list: List[Dict[str, object]] = []

        index = 0
        shape_geometry = Geometry(self.area_shape, self.crs).transform(CRS.WGS84)

        for utm_cell in self.utm_grid:
            utm_cell_geom, utm_cell_prop = utm_cell
            # the UTM MGRS grid definition contains four 0 zones at the poles (0A, 0B, 0Y, 0Z)
            if utm_cell_prop["zone"] == 0:
                continue
            utm_crs = self._get_utm_from_props(utm_cell_prop)
            cell_info = dict(
                crs=utm_crs.name,
                utm_zone=str(utm_cell_prop["zone"]).zfill(2),
                utm_row=utm_cell_prop["row"],
                direction=utm_cell_prop["direction"],
            )

            intersection = utm_cell_geom.intersection(shape_geometry.geometry)

            if not intersection.is_empty and isinstance(intersection, GeometryCollection):
                intersection = MultiPolygon(
                    geo_object for geo_object in intersection if isinstance(geo_object, (Polygon, MultiPolygon))
                )

            if not intersection.is_empty:
                intersection = Geometry(intersection, CRS.WGS84).transform(utm_crs)

                bbox_partition = self._align_bbox_to_size(intersection.bbox).get_partition(size_x=size_x, size_y=size_y)

                for i, column in enumerate(bbox_partition):
                    for j, part in enumerate(column):
                        if part.geometry.intersects(intersection.geometry):
                            bbox_list.append(part)
                            info_list.append(dict(**cell_info, index=index, index_x=i, index_y=j))
                            index += 1

        return bbox_list, info_list

    def get_bbox_list(  # type: ignore[override]
        self, buffer: Union[None, float, Tuple[float, float]] = None
    ) -> List[BBox]:
        """Get list of bounding boxes.

        The CRS is fixed to the computed UTM CRS. This BBox splitter does not support reducing size of output
        bounding boxes

        :param buffer: A percentage of each BBox size increase. This will cause neighbouring bounding boxes to overlap.
        :return: List of bounding boxes
        """
        return super().get_bbox_list(buffer=buffer)


class UtmGridSplitter(BaseUtmSplitter):
    """Splitter that returns bounding boxes of fixed size aligned to the UTM MGRS grid"""

    def _get_utm_polygons(self) -> List[Tuple[BaseGeometry, Dict[str, Any]]]:
        """Find UTM grid zones overlapping with input area shape

        :return: List of geometries and properties of UTM grid zones overlapping with input area shape
        """
        # file downloaded from faculty.baruch.cuny.edu/geoportal/data/esri/world/utmzone.zip
        utm_grid_filename = os.path.join(os.path.dirname(__file__), ".utmzones.geojson")

        if not os.path.isfile(utm_grid_filename):
            raise IOError(f"UTM grid definition file does not exist: {os.path.abspath(utm_grid_filename)}")

        with open(utm_grid_filename) as utm_grid_file:
            utm_grid = json.load(utm_grid_file)["features"]

        utm_geom_list = [shapely.geometry.shape(utm_zone["geometry"]) for utm_zone in utm_grid]
        utm_prop_list = [
            dict(
                zone=utm_zone["properties"]["ZONE"],
                row=utm_zone["properties"]["ROW_"],
                direction="N" if utm_zone["properties"]["ROW_"] >= "N" else "S",
            )
            for utm_zone in utm_grid
        ]

        return list(zip(utm_geom_list, utm_prop_list))


class UtmZoneSplitter(BaseUtmSplitter):
    """Splitter that returns bounding boxes of fixed size aligned to the equator and the UTM zones."""

    LNG_MIN, LNG_MAX, LNG_UTM = -180, 180, 6
    LAT_MIN, LAT_MAX, LAT_EQ = -80, 84, 0

    def _get_utm_polygons(self) -> List[Tuple[BaseGeometry, Dict[str, Any]]]:
        """Find UTM zones overlapping with input area shape

        The returned geometry corresponds to a triangle ranging from the equator to the North/South Pole

        :return: List of geometries and properties of UTM zones overlapping with input area shape
        """
        utm_geom_list = []
        for lat in [(self.LAT_EQ, self.LAT_MAX), (self.LAT_MIN, self.LAT_EQ)]:
            for lng in range(self.LNG_MIN, self.LNG_MAX, self.LNG_UTM):
                points = []
                # A new point is added per each degree - this is inline with geometries used by UtmGridSplitter
                # In the future the number of points will be calculated according to bbox_size parameter
                for degree in range(lat[0], lat[1]):
                    points.append((lng, degree))
                for degree in range(lng, lng + self.LNG_UTM):
                    points.append((degree, lat[1]))
                for degree in range(lat[1], lat[0], -1):
                    points.append((lng + self.LNG_UTM, degree))
                for degree in range(lng + self.LNG_UTM, lng, -1):
                    points.append((degree, lat[0]))

                utm_geom_list.append(Polygon(points))

        utm_prop_list = [
            dict(zone=zone, row="", direction=direction) for direction in ["N", "S"] for zone in range(1, 61)
        ]

        return list(zip(utm_geom_list, utm_prop_list))


class BatchSplitter(AreaSplitter):
    """A splitter that obtains split bounding boxes from Sentinel Hub Batch API"""

    def __init__(
        self,
        *,
        request_id: Optional[str] = None,
        batch_request: Optional[BatchRequest] = None,
        config: Optional[SHConfig] = None,
    ):
        """
        :param request_id: An ID of a batch request
        :param batch_request: A batch request object. It is an alternative to the `request_id` parameter
        :param config: A configuration object with credentials and information about which service deployment to
            use.
        """
        self.batch_client = SentinelHubBatch(config=config)

        if batch_request is None:
            if request_id is None:
                raise ValueError("One of the parameters request_id and batch_request has to be given")
            batch_request = self.batch_client.get_request(request_id)

        self.batch_request = batch_request
        self.tile_size = self._get_tile_size()
        self.tile_buffer = self._get_tile_buffer()

        batch_geometry = batch_request.geometry
        super().__init__([batch_geometry.geometry], batch_geometry.crs)

    def _get_tile_size(self) -> Tuple[float, float]:
        """Collects a tile size from the tiling grid info in units of the grid CRS."""
        tiling_grid_id = self.batch_request.tiling_grid["id"]
        grid_info = self.batch_client.get_tiling_grid(tiling_grid_id)

        return grid_info["properties"]["tileWidth"], grid_info["properties"]["tileHeight"]

    def _get_tile_buffer(self) -> Tuple[float, float]:
        """Calculates tile buffer in units of the grid CRS."""
        grid_info = self.batch_request.tiling_grid
        resolution = grid_info["resolution"]
        return grid_info.get("bufferX", 0) * resolution, grid_info.get("bufferY", 0) * resolution

    def _make_split(self) -> Tuple[List[BBox], List[Dict[str, object]]]:
        """This method actually loads bounding boxes from the service and prepares the lists"""
        tile_info_list = list(self.batch_client.iter_tiles(self.batch_request))

        bbox_list = [self._reconstruct_bbox(tile_info) for tile_info in tile_info_list]
        info_list = [
            {key: value for key, value in tile_info.items() if key != "geometry"} for tile_info in tile_info_list
        ]

        return bbox_list, info_list

    def _reconstruct_bbox(self, tile_info: JsonDict) -> BBox:
        """Reconstructs a bounding box from tile and grid properties."""
        tile_crs = CRS(tile_info["origin"]["crs"]["properties"]["name"])

        upper_left_corner = tile_info["origin"]["coordinates"]
        width, height = self.tile_size

        return BBox(
            [
                upper_left_corner[0] - self.tile_buffer[0],
                upper_left_corner[1] - height - self.tile_buffer[1],
                upper_left_corner[0] + width + self.tile_buffer[0],
                upper_left_corner[1] + self.tile_buffer[1],
            ],
            tile_crs,
        )


def _parse_to_pair(
    parameter: Union[T, Tuple[T, T]], allowed_types: Tuple[type, ...], param_name: str = ""
) -> Tuple[T, T]:
    """Parses the parameters defining the splitting of the BBox."""

    if isinstance(parameter, (tuple, list)) and len(parameter) == 2:
        split_x, split_y = parameter
        if isinstance(split_x, allowed_types) and isinstance(split_y, allowed_types):
            return split_x, split_y

    parameter = cast(T, parameter)
    if isinstance(parameter, allowed_types):
        return parameter, parameter

    raise ValueError(
        f"Parameter {param_name} must be a single instance or a pair, with allowed types {allowed_types}, but"
        f" {parameter} was given"
    )

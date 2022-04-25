"""
Module implementing geometry classes
"""
from abc import ABCMeta, abstractmethod
from math import ceil
from typing import Iterator, List, Optional, Tuple, Union

import shapely.geometry
import shapely.ops
import shapely.wkt
from shapely.geometry import MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry

from .constants import CRS
from .geo_utils import transform_point


class _BaseGeometry(metaclass=ABCMeta):
    """Base geometry class"""

    def __init__(self, crs: CRS):
        """
        :param crs: Coordinate reference system of the geometry
        """
        self._crs = CRS(crs)

    @property
    def crs(self) -> CRS:
        """Returns the coordinate reference system (CRS)

        :return: Coordinate reference system Enum
        """
        return self._crs

    @property
    @abstractmethod
    def geometry(self) -> Union[Polygon, MultiPolygon]:
        """An abstract property - every subclass must implement geometry property"""

    @property
    def geojson(self) -> dict:
        """Returns representation in a GeoJSON format. Use `json.dump` for writing it to file.

        :return: A dictionary in GeoJSON format
        """
        return self.get_geojson(with_crs=True)

    def get_geojson(self, with_crs: bool = True) -> dict:
        """Returns representation in a GeoJSON format. Use `json.dump` for writing it to file.

        :param with_crs: A flag indicating if GeoJSON dictionary should contain CRS part
        :return: A dictionary in GeoJSON format
        """
        geometry_geojson = shapely.geometry.mapping(self.geometry)

        if with_crs:
            return {**self._crs_to_geojson(), **geometry_geojson}
        return geometry_geojson

    def _crs_to_geojson(self) -> dict:
        """Helper method which generates part of GeoJSON format related to CRS"""
        return {"crs": {"type": "name", "properties": {"name": f"urn:ogc:def:crs:EPSG::{self.crs.value}"}}}

    @property
    def wkt(self) -> str:
        """Transforms geometry object into `Well-known text` format

        :return: string in WKT format
        """
        return self.geometry.wkt


class BBox(_BaseGeometry):
    """Class representing a bounding box in a given CRS.

    Throughout the sentinelhub package this class serves as the canonical representation of a bounding
    box. It can initialize itself from multiple representations:

        1) ``((min_x,min_y),(max_x,max_y))``,
        2) ``(min_x,min_y,max_x,max_y)``,
        3) ``[min_x,min_y,max_x,max_y]``,
        4) ``[[min_x, min_y],[max_x,max_y]]``,
        5) ``[(min_x, min_y),(max_x,max_y)]``,
        6) ``([min_x, min_y],[max_x,max_y])``,
        7) ``'min_x,min_y,max_x,max_y'``,
        8) ``{'min_x':min_x, 'max_x':max_x, 'min_y':min_y, 'max_y':max_y}``,
        9) ``bbox``, where ``bbox`` is an instance of ``BBox``.

    Note that BBox coordinate system depends on ``crs`` parameter:

    - In case of ``constants.CRS.WGS84`` axis x represents longitude and axis y represents latitude.
    - In case of ``constants.CRS.POP_WEB`` axis x represents easting and axis y represents northing.
    - In case of ``constants.CRS.UTM_*`` axis x represents easting and axis y represents northing.
    """

    def __init__(self, bbox: Union["BBox", tuple, list, dict, str, BaseGeometry], crs: CRS):
        """
        :param bbox: A bbox in any valid representation
        :param crs: Coordinate reference system of the bounding box
        """
        x_fst, y_fst, x_snd, y_snd = BBox._to_tuple(bbox)
        self.min_x = min(x_fst, x_snd)
        self.max_x = max(x_fst, x_snd)
        self.min_y = min(y_fst, y_snd)
        self.max_y = max(y_fst, y_snd)

        super().__init__(crs)

    def __iter__(self) -> Iterator[float]:
        """This method enables iteration over coordinates of bounding box"""
        return iter(self.lower_left + self.upper_right)

    def __repr__(self) -> str:
        """Class representation"""
        return f"{self.__class__.__name__}(({self.lower_left}, {self.upper_right}), crs={repr(self.crs)})"

    def __str__(self, reverse: bool = False) -> str:
        """Transforms bounding box into a string of coordinates

        :param reverse: `True` if x and y coordinates should be switched and `False` otherwise
        :return: String of coordinates
        """
        if reverse:
            return f"{self.min_y},{self.min_x},{self.max_y},{self.max_x}"
        return f"{self.min_x},{self.min_y},{self.max_x},{self.max_y}"

    def __eq__(self, other: object) -> bool:
        """Method for comparing two bounding boxes

        :param other: Another bounding box object
        :return: `True` if bounding boxes have the same coordinates and the same CRS and `False otherwise
        """
        if isinstance(other, BBox):
            return list(self) == list(other) and self.crs is other.crs
        return False

    @property
    def lower_left(self) -> Tuple[float, float]:
        """Returns the lower left vertex of the bounding box

        :return: min_x, min_y
        """
        return self.min_x, self.min_y

    @property
    def upper_right(self) -> Tuple[float, float]:
        """Returns the upper right vertex of the bounding box

        :return: max_x, max_y
        """
        return self.max_x, self.max_y

    @property
    def middle(self) -> Tuple[float, float]:
        """Returns the middle point of the bounding box

        :return: middle point
        """
        return (self.min_x + self.max_x) / 2, (self.min_y + self.max_y) / 2

    def reverse(self) -> "BBox":
        """Returns a new BBox object where x and y coordinates are switched

        :return: New BBox object with switched coordinates
        """
        return BBox((self.min_y, self.min_x, self.max_y, self.max_x), crs=self.crs)

    def transform(self, crs: CRS, always_xy: bool = True) -> "BBox":
        """Transforms BBox from current CRS to target CRS

        This transformation will take lower left and upper right corners of the bounding box, transform these 2 points
        and define a new bounding box with them. The resulting bounding box might not completely cover the original
        bounding box but at least the transformation is reversible.

        :param crs: target CRS
        :param always_xy: Parameter that is passed to `pyproj.Transformer` object and defines axis order for
            transformation. The default value `True` is in most cases the correct one.
        :return: Bounding box in target CRS
        """
        new_crs = CRS(crs)
        return BBox(
            (
                transform_point(self.lower_left, self.crs, new_crs, always_xy=always_xy),
                transform_point(self.upper_right, self.crs, new_crs, always_xy=always_xy),
            ),
            crs=new_crs,
        )

    def transform_bounds(self, crs: CRS, always_xy: bool = True) -> "BBox":
        """Alternative way to transform BBox from current CRS to target CRS.

        This transformation will transform the bounding box geometry to another CRS as a geometric object, and then
        define a new bounding box from boundaries of that geometry. The resulting bounding box might be larger than
        original bounding box, but it will always completely cover it.

        :param crs: target CRS
        :param always_xy: Parameter that is passed to `pyproj.Transformer` object and defines axis order for
            transformation. The default value `True` is in most cases the correct one.
        :return: Bounding box in target CRS
        """
        bbox_geometry = Geometry(self.geometry, self.crs)
        bbox_geometry = bbox_geometry.transform(crs, always_xy=always_xy)
        return bbox_geometry.bbox

    def buffer(self, buffer: Union[float, Tuple[float, float]], *, relative: bool = True) -> "BBox":
        """Provides a new bounding box with a size that is changed either by a relative or an absolute buffer.

        :param buffer: The buffer can be provided either as a single number or a tuple of 2 numbers, one for buffer in
            horizontal direction and one for buffer in vertical direction. The buffer can also be negative as long as
            this doesn't reduce the bounding box into nothing.
        :param relative: If `True` the given buffer values will be interpreted as a percentage of distance between
            bounding box center point and its side edge (not to distance between opposite sides!). If `False` the given
            buffer will be interpreted as an absolute buffer measured in bounding box coordinate units.
        :return: A new bounding box of buffered size.
        """
        if isinstance(buffer, tuple):
            buffer_x, buffer_y = buffer
        elif isinstance(buffer, (int, float)):
            buffer_x, buffer_y = buffer, buffer
        else:
            raise ValueError(f"Buffer should be a number or a tuple of 2 numbers, got {type(buffer)}")

        size_x, size_y = self.max_x - self.min_x, self.max_y - self.min_y

        if relative:
            buffer_x = buffer_x * size_x / 2
            buffer_y = buffer_y * size_y / 2

        for absolute_buffer, size, direction in [(buffer_x, size_x, "horizontal"), (buffer_y, size_y, "vertical")]:
            if 2 * absolute_buffer + size <= 0:
                raise ValueError(
                    f"Negative buffer is too large, cannot reduce the bounding box to nothing in {direction} direction"
                )

        return BBox(
            (
                self.min_x - buffer_x,
                self.min_y - buffer_y,
                self.max_x + buffer_x,
                self.max_y + buffer_y,
            ),
            self.crs,
        )

    def get_polygon(self, reverse: bool = False) -> Tuple[Tuple[float, float], ...]:
        """Returns a tuple of coordinates of 5 points describing a polygon. Points are listed in clockwise order, first
        point is the same as the last.

        :param reverse: `True` if x and y coordinates should be switched and `False` otherwise
        :return: `((x_1, y_1), ... , (x_5, y_5))`
        """
        bbox = self.reverse() if reverse else self
        polygon = (
            (bbox.min_x, bbox.min_y),
            (bbox.min_x, bbox.max_y),
            (bbox.max_x, bbox.max_y),
            (bbox.max_x, bbox.min_y),
            (bbox.min_x, bbox.min_y),
        )
        return polygon

    @property
    def geometry(self) -> shapely.geometry.Polygon:
        """Returns polygon geometry in shapely format

        :return: A polygon in shapely format
        """
        return shapely.geometry.Polygon(self.get_polygon())

    def get_partition(
        self,
        num_x: Optional[int] = None,
        num_y: Optional[int] = None,
        size_x: Optional[float] = None,
        size_y: Optional[float] = None,
    ) -> List[List["BBox"]]:
        """Partitions bounding box into smaller bounding boxes of the same size.

        If `num_x` and `num_y` are specified, the total number of BBoxes is know but not the size. If `size_x` and
        `size_y` are provided, the BBox size is fixed but the number of BBoxes is not known in advance. In the latter
        case, the generated bounding boxes might cover an area larger than the parent BBox.

        :param num_x: Number of parts BBox will be horizontally divided into.
        :param num_y: Number of parts BBox will be vertically divided into.
        :param size_x: Physical dimension of BBox along easting coordinate
        :param size_y: Physical dimension of BBox along northing coordinate
        :return: Two-dimensional list of smaller bounding boxes. Their location is
        """
        if (num_x is not None and num_y is not None) and (size_x is None and size_y is None):
            size_x, size_y = (self.max_x - self.min_x) / num_x, (self.max_y - self.min_y) / num_y
        elif (size_x is not None and size_y is not None) and (num_x is None and num_y is None):
            num_x, num_y = ceil((self.max_x - self.min_x) / size_x), ceil((self.max_y - self.min_y) / size_y)
        else:
            raise ValueError("Not supported partition. Either (num_x, num_y) or (size_x, size_y) must be specified")

        return [
            [
                BBox(
                    [
                        self.min_x + i * size_x,
                        self.min_y + j * size_y,
                        self.min_x + (i + 1) * size_x,
                        self.min_y + (j + 1) * size_y,
                    ],
                    crs=self.crs,
                )
                for j in range(num_y)
            ]
            for i in range(num_x)
        ]

    def get_transform_vector(self, resx: float, resy: float) -> Tuple[float, float, float, float, float, float]:
        """Given resolution it returns a transformation vector

        :param resx: Resolution in x direction
        :param resy: Resolution in y direction
        :return: A tuple with 6 numbers representing transformation vector
        """
        return self.min_x, self._parse_resolution(resx), 0, self.max_y, 0, -self._parse_resolution(resy)

    @staticmethod
    def _parse_resolution(res: Union[str, int, float]) -> float:
        """Helper method for parsing given resolution. It will also try to parse a string into float

        :return: A float value of resolution
        """
        if isinstance(res, str):
            return float(res.strip("m"))
        if isinstance(res, (int, float)):
            return float(res)

        raise TypeError(f"Resolution should be a float, got resolution of type {type(res)}")

    @staticmethod
    def _to_tuple(bbox: Union["BBox", tuple, list, dict, str, BaseGeometry]) -> Tuple[float, float, float, float]:
        """Converts the input bbox representation (see the constructor docstring for a list of valid representations)
        into a flat tuple

        :param bbox: A bbox in one of 7 forms listed in the class description.
        :return: A flat tuple of size
        :raises: TypeError
        """
        if isinstance(bbox, (list, tuple)):
            return BBox._tuple_from_list_or_tuple(bbox)
        if isinstance(bbox, str):
            return BBox._tuple_from_str(bbox)
        if isinstance(bbox, dict):
            return BBox._tuple_from_dict(bbox)
        if isinstance(bbox, BBox):
            return BBox._tuple_from_bbox(bbox)
        if isinstance(bbox, BaseGeometry):
            return bbox.bounds
        raise TypeError("Invalid bbox representation")

    @staticmethod
    def _tuple_from_list_or_tuple(bbox: Union[list, tuple]) -> Tuple[float, float, float, float]:
        """Converts a list or tuple representation of a bbox into a flat tuple representation.

        :param bbox: a list or tuple with 4 coordinates that is either flat or nested
        :return: tuple (min_x,min_y,max_x,max_y)
        :raises: TypeError
        """
        if len(bbox) == 4:
            return tuple(map(float, bbox))  # type: ignore # iterator length
        if len(bbox) == 2 and all(isinstance(point, (list, tuple)) for point in bbox):
            return BBox._tuple_from_list_or_tuple(bbox[0] + bbox[1])
        raise TypeError("Expected a valid list or tuple representation of a bbox")

    @staticmethod
    def _tuple_from_str(bbox: str) -> Tuple[float, float, float, float]:
        """Parses a string of numbers separated by any combination of commas and spaces

        :param bbox: e.g. str of the form `min_x ,min_y  max_x, max_y`
        :return: tuple (min_x,min_y,max_x,max_y)
        """
        return tuple(float(s) for s in bbox.replace(",", " ").split() if s)  # type: ignore # iterator length

    @staticmethod
    def _tuple_from_dict(bbox: dict) -> Tuple[float, float, float, float]:
        """Converts a dictionary representation of a bbox into a flat tuple representation

        :param bbox: a dict with keys "min_x, "min_y", "max_x", and "max_y"
        :return: tuple (min_x,min_y,max_x,max_y)
        :raises: KeyError
        """
        return bbox["min_x"], bbox["min_y"], bbox["max_x"], bbox["max_y"]

    @staticmethod
    def _tuple_from_bbox(bbox: "BBox") -> Tuple[float, float, float, float]:
        """Converts a BBox instance into a tuple

        :param bbox: An instance of the BBox type
        :return: tuple (min_x, min_y, max_x, max_y)
        """
        return bbox.lower_left + bbox.upper_right


class Geometry(_BaseGeometry):
    """A class that combines shapely geometry with coordinate reference system. It currently supports polygons and
    multipolygons.

    It can be initialized with any of the following geometry representations:
    - `shapely.geometry.Polygon` or `shapely.geometry.MultiPolygon`
    - A GeoJSON dictionary with (multi)polygon coordinates
    - A WKT string with (multi)polygon coordinates
    """

    def __init__(self, geometry: Union[Polygon, MultiPolygon, dict, str], crs: CRS):
        """
        :param geometry: A polygon or multipolygon in any valid representation
        :param crs: Coordinate reference system of the geometry
        """
        self._geometry = self._parse_geometry(geometry)

        super().__init__(crs)

    def __repr__(self) -> str:
        """Method for class representation"""
        return f"{self.__class__.__name__}({self.wkt}, crs={repr(self.crs)})"

    def __eq__(self, other: object) -> bool:
        """Method for comparing two Geometry classes

        :param other: Another Geometry object
        :return: `True` if geometry objects have the same geometry and CRS and `False` otherwise
        """
        if isinstance(other, Geometry):
            return self.geometry == other.geometry and self.crs is other.crs
        return False

    def reverse(self) -> "Geometry":
        """Returns a new Geometry object where x and y coordinates are switched

        :return: New Geometry object with switched coordinates
        """
        return Geometry(shapely.ops.transform(lambda x, y: (y, x), self.geometry), crs=self.crs)

    def transform(self, crs: CRS, always_xy: bool = True) -> "Geometry":
        """Transforms Geometry from current CRS to target CRS

        :param crs: target CRS
        :param always_xy: Parameter that is passed to `pyproj.Transformer` object and defines axis order for
            transformation. The default value `True` is in most cases the correct one.
        :return: Geometry in target CRS
        """
        new_crs = CRS(crs)

        geometry = self.geometry
        if new_crs is not self.crs:
            transform_function = self.crs.get_transform_function(new_crs, always_xy=always_xy)
            geometry = shapely.ops.transform(transform_function, geometry)

        return Geometry(geometry, crs=new_crs)

    @classmethod
    def from_geojson(cls, geojson: dict, crs: Optional[CRS] = None) -> "Geometry":
        """Create Geometry object from geojson. It will parse crs from geojson (if info is available),
        otherwise it will be set to crs (WGS84 if parameter is empty)

        :param geojson: geojson geometry (single feature)
        :param crs: crs to be used if not available in geojson, CRS.WGS84 if not provided
        :return: Geometry object
        """
        try:
            crs = CRS(geojson["crs"]["properties"]["name"])
        except (KeyError, AttributeError, TypeError):
            pass

        if not crs:
            crs = CRS.WGS84

        return cls(geojson, crs=crs)

    @property
    def geometry(self) -> Union[Polygon, MultiPolygon]:
        """Returns shapely object representing geometry in this class

        :return: A polygon or a multipolygon in shapely format
        """
        return self._geometry

    @property
    def bbox(self) -> BBox:
        """Returns BBox object representing bounding box around the geometry

        :return: A bounding box, with same CRS
        """
        return BBox(self.geometry, self.crs)

    @staticmethod
    def _parse_geometry(geometry: Union[Polygon, MultiPolygon, dict, str]) -> Union[Polygon, MultiPolygon]:
        """Parses given geometry into shapely object

        :param geometry: A representation of the geometry
        :return: Shapely polygon or multipolygon
        :raises TypeError
        """
        if isinstance(geometry, str):
            geometry = shapely.wkt.loads(geometry)
        elif isinstance(geometry, dict):
            geometry = shapely.geometry.shape(geometry)
        elif not isinstance(geometry, BaseGeometry):
            raise TypeError("Unsupported geometry representation")

        if not isinstance(geometry, (Polygon, MultiPolygon)):
            raise ValueError(f"Supported geometry types are polygon and multipolygon, got {type(geometry)}")

        return geometry


class BBoxCollection(_BaseGeometry):
    """A collection of bounding boxes"""

    def __init__(self, bbox_list: Union["BBoxCollection", List[BBox]]):
        """
        :param bbox_list: A list of BBox objects which have to be in the same CRS
        """
        self._bbox_list, crs = self._parse_bbox_list(bbox_list)
        self._geometry = self._get_geometry()

        super().__init__(crs)

    def __repr__(self) -> str:
        """Method for class representation"""
        bbox_list_repr = ", ".join([repr(bbox) for bbox in self.bbox_list])
        return f"{self.__class__.__name__}({bbox_list_repr})"

    def __eq__(self, other: object) -> bool:
        """Method for comparing two BBoxCollection classes"""
        if isinstance(other, BBoxCollection):
            return (
                self.crs is other.crs
                and len(self.bbox_list) == len(other.bbox_list)
                and all(bbox == other_bbox for bbox, other_bbox in zip(self, other))
            )
        return False

    def __iter__(self) -> Iterator[BBox]:
        """This method enables iteration over bounding boxes in collection"""
        return iter(self.bbox_list)

    @property
    def bbox_list(self) -> List[BBox]:
        """Returns the list of bounding boxes from collection

        :return: The list of bounding boxes
        """
        return self._bbox_list

    @property
    def geometry(self) -> MultiPolygon:
        """Returns shapely object representing geometry

        :return: A multipolygon of bounding boxes
        """
        return self._geometry

    @property
    def bbox(self) -> BBox:
        """Returns BBox object representing bounding box around the geometry

        :return: A bounding box, with same CRS
        """
        return BBox(self.geometry, self.crs)

    def reverse(self) -> "BBoxCollection":
        """Returns a new BBoxCollection object where all x and y coordinates are switched

        :return: New Geometry object with switched coordinates
        """
        return BBoxCollection([bbox.reverse() for bbox in self.bbox_list])

    def transform(self, crs: CRS, always_xy: bool = True) -> "BBoxCollection":
        """Transforms BBoxCollection from current CRS to target CRS

        :param crs: target CRS
        :param always_xy: Parameter that is passed to `pyproj.Transformer` object and defines axis order for
            transformation. The default value `True` is in most cases the correct one.
        :return: BBoxCollection in target CRS
        """
        return BBoxCollection([bbox.transform(crs, always_xy=always_xy) for bbox in self.bbox_list])

    def _get_geometry(self) -> MultiPolygon:
        """Creates a multipolygon of bounding box polygons"""
        return MultiPolygon([bbox.geometry for bbox in self.bbox_list])

    @staticmethod
    def _parse_bbox_list(bbox_list: Union["BBoxCollection", List[BBox]]) -> Tuple[List[BBox], CRS]:
        """Helper method for parsing a list of bounding boxes"""
        if isinstance(bbox_list, BBoxCollection):
            return bbox_list.bbox_list, bbox_list.crs

        if not isinstance(bbox_list, list) or not bbox_list:
            raise ValueError("Expected non-empty list of BBox objects")

        for bbox in bbox_list:
            if not isinstance(bbox, BBox):
                raise ValueError(f"Elements in the list should be of type {BBox.__name__}, got {type(bbox)}")

        crs = bbox_list[0].crs
        for bbox in bbox_list:
            if bbox.crs is not crs:
                raise ValueError("All bounding boxes should have the same CRS")

        return bbox_list, crs

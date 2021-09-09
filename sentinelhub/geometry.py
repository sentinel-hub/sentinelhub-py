"""
Module implementing geometry classes
"""
from abc import ABC, abstractmethod
from math import ceil

import shapely.geometry
import shapely.ops
import shapely.wkt

from .constants import CRS
from .geo_utils import transform_point


class BaseGeometry(ABC):
    """ Base geometry class
    """
    def __init__(self, crs):
        """
        :param crs: Coordinate reference system of the geometry
        :type crs: constants.CRS
        """
        self._crs = CRS(crs)

    @property
    def crs(self):
        """ Returns the coordinate reference system (CRS)

        :return: Coordinate reference system Enum
        :rtype: constants.CRS
        """
        return self._crs

    @property
    @abstractmethod
    def geometry(self):
        """ An abstract property - ever subclass must implement geometry property
        """
        raise NotImplementedError

    @property
    def geojson(self):
        """ Returns representation in a GeoJSON format. Use json.dump for writing it to file.

        :return: A dictionary in GeoJSON format
        :rtype: dict
        """
        return self.get_geojson(with_crs=True)

    def get_geojson(self, with_crs=True):
        """ Returns representation in a GeoJSON format. Use json.dump for writing it to file.

        :param with_crs: A flag indicating if GeoJSON dictionary should contain CRS part
        :type with_crs: bool
        :return: A dictionary in GeoJSON format
        :rtype: dict
        """
        geometry_geojson = shapely.geometry.mapping(self.geometry)

        if with_crs:
            return {
                **self._crs_to_geojson(),
                **geometry_geojson
            }
        return geometry_geojson

    def _crs_to_geojson(self):
        """ Helper method which generates part of GeoJSON format related to CRS
        """
        return {
            'crs': {
                'type': 'name',
                'properties': {'name': f'urn:ogc:def:crs:EPSG::{self.crs.value}'}
            }
        }

    @property
    def wkt(self):
        """ Transforms geometry object into `Well-known text` format

        :return: string in WKT format
        :rtype: str
        """
        return self.geometry.wkt


class BBox(BaseGeometry):
    """ Class representing a bounding box in a given CRS.

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

    - In case of ``constants.CRS.WGS84`` axis x represents longitude and axis y represents latitude
    - In case of ``constants.CRS.POP_WEB`` axis x represents easting and axis y represents northing
    - In case of ``constants.CRS.UTM_*`` axis x represents easting and axis y represents northing
    """
    def __init__(self, bbox, crs):
        """
        :param bbox: A bbox in any valid representation
        :param crs: Coordinate reference system of the bounding box
        :type crs: constants.CRS
        """
        x_fst, y_fst, x_snd, y_snd = BBox._to_tuple(bbox)
        self.min_x = min(x_fst, x_snd)
        self.max_x = max(x_fst, x_snd)
        self.min_y = min(y_fst, y_snd)
        self.max_y = max(y_fst, y_snd)

        super().__init__(crs)

    def __iter__(self):
        """ This method enables iteration over coordinates of bounding box
        """
        return iter(self.lower_left + self.upper_right)

    def __repr__(self):
        """ Class representation
        """
        return f'{self.__class__.__name__}(({self.lower_left}, {self.upper_right}), crs={repr(self.crs)})'

    def __str__(self, reverse=False):
        """ Transforms bounding box into a string of coordinates

        :param reverse: `True` if x and y coordinates should be switched and `False` otherwise
        :type reverse: bool
        :return: String of coordinates
        :rtype: str
        """
        if reverse:
            return f'{self.min_y},{self.min_x},{self.max_y},{self.max_x}'
        return f'{self.min_x},{self.min_y},{self.max_x},{self.max_y}'

    def __eq__(self, other):
        """ Method for comparing two bounding boxes

        :param other: Another bounding box object
        :type other: BBox
        :return: `True` if bounding boxes have the same coordinates and the same CRS and `False otherwise
        :rtype: bool
        """
        if not isinstance(other, BBox):
            return False
        return list(self) == list(other) and self.crs is other.crs

    @property
    def lower_left(self):
        """ Returns the lower left vertex of the bounding box

        :return: min_x, min_y
        :rtype: (float, float)
        """
        return self.min_x, self.min_y

    @property
    def upper_right(self):
        """ Returns the upper right vertex of the bounding box

        :return: max_x, max_y
        :rtype: (float, float)
        """
        return self.max_x, self.max_y

    @property
    def middle(self):
        """ Returns the middle point of the bounding box

        :return: middle point
        :rtype: (float, float)
        """
        return (self.min_x + self.max_x) / 2, (self.min_y + self.max_y) / 2

    def reverse(self):
        """ Returns a new BBox object where x and y coordinates are switched

        :return: New BBox object with switched coordinates
        :rtype: BBox
        """
        return BBox((self.min_y, self.min_x, self.max_y, self.max_x), crs=self.crs)

    def transform(self, crs, always_xy=True):
        """ Transforms BBox from current CRS to target CRS

        This transformation will take lower left and upper right corners of the bounding box, transform these 2 points
        and define a new bounding box with them. The resulting bounding box might not completely cover the original
        bounding box but at least the transformation is reversible.

        :param crs: target CRS
        :type crs: constants.CRS
        :param always_xy: Parameter that is passed to `pyproj.Transformer` object and defines axis order for
            transformation. The default value `True` is in most cases the correct one.
        :type always_xy: bool
        :return: Bounding box in target CRS
        :rtype: BBox
        """
        new_crs = CRS(crs)
        return BBox((transform_point(self.lower_left, self.crs, new_crs, always_xy=always_xy),
                     transform_point(self.upper_right, self.crs, new_crs, always_xy=always_xy)), crs=new_crs)

    def transform_bounds(self, crs, always_xy=True):
        """ Alternative way to transform BBox from current CRS to target CRS.

        This transformation will transform the bounding box geometry to another CRS as a geometric object, and then
        define a new bounding box from boundaries of that geometry. The resulting bounding box might be larger than
        original bounding box but it will always completely cover it.

        :param crs: target CRS
        :type crs: constants.CRS
        :param always_xy: Parameter that is passed to `pyproj.Transformer` object and defines axis order for
            transformation. The default value `True` is in most cases the correct one.
        :type always_xy: bool
        :return: Bounding box in target CRS
        :rtype: BBox
        """
        bbox_geometry = Geometry(self.geometry, self.crs)
        bbox_geometry = bbox_geometry.transform(crs, always_xy=always_xy)
        return bbox_geometry.bbox

    def buffer(self, buffer):
        """ Changes both BBox dimensions (width and height) by a percentage of size of each dimension. If number is
        negative, the size will decrease. Returns a new instance of BBox object.

        :param buffer: A percentage of BBox size change
        :type buffer: float
        :return: A new bounding box of buffered size
        :rtype: BBox
        """
        if buffer < -1:
            raise ValueError('Cannot reduce the bounding box to nothing, buffer must be >= -1.0')
        ratio = 1 + buffer
        mid_x, mid_y = self.middle
        return BBox((mid_x - (mid_x - self.min_x) * ratio, mid_y - (mid_y - self.min_y) * ratio,
                     mid_x + (self.max_x - mid_x) * ratio, mid_y + (self.max_y - mid_y) * ratio), self.crs)

    def get_polygon(self, reverse=False):
        """ Returns a tuple of coordinates of 5 points describing a polygon. Points are listed in clockwise order, first
        point is the same as the last.

        :param reverse: `True` if x and y coordinates should be switched and `False` otherwise
        :type reverse: bool
        :return: `((x_1, y_1), ... , (x_5, y_5))`
        :rtype: tuple(tuple(float))
        """
        bbox = self.reverse() if reverse else self
        polygon = ((bbox.min_x, bbox.min_y),
                   (bbox.min_x, bbox.max_y),
                   (bbox.max_x, bbox.max_y),
                   (bbox.max_x, bbox.min_y),
                   (bbox.min_x, bbox.min_y))
        return polygon

    @property
    def geometry(self):
        """ Returns polygon geometry in shapely format

        :return: A polygon in shapely format
        :rtype: shapely.geometry.polygon.Polygon
        """
        return shapely.geometry.Polygon(self.get_polygon())

    def get_partition(self, num_x=None, num_y=None, size_x=None, size_y=None):
        """ Partitions bounding box into smaller bounding boxes of the same size.

        If `num_x` and `num_y` are specified, the total number of BBoxes is know but not the size. If `size_x` and
        `size_y` are provided, the BBox size is fixed but the number of BBoxes is not known in advance. In the latter
        case, the generated bounding boxes might cover an area larger than the parent BBox.

        :param num_x: Number of parts BBox will be horizontally divided into.
        :type num_x: int or None
        :param num_y: Number of parts BBox will be vertically divided into.
        :type num_y: int or None
        :param size_x: Physical dimension of BBox along easting coordinate
        :type size_x: float or None
        :param size_y: Physical dimension of BBox along northing coordinate
        :type size_y: float or None
        :return: Two-dimensional list of smaller bounding boxes. Their location is
        :rtype: list(list(BBox))
        """
        if (num_x is not None and num_y is not None) and (size_x is None and size_y is None):
            size_x, size_y = (self.max_x - self.min_x) / num_x, (self.max_y - self.min_y) / num_y
        elif (size_x is not None and size_y is not None) and (num_x is None and num_y is None):
            num_x, num_y = ceil((self.max_x - self.min_x) / size_x), ceil((self.max_y - self.min_y) / size_y)
        else:
            raise ValueError('Not supported partition. Either (num_x, num_y) or (size_x, size_y) must be specified')

        return [[BBox([self.min_x + i * size_x, self.min_y + j * size_y,
                       self.min_x + (i + 1) * size_x, self.min_y + (j + 1) * size_y],
                      crs=self.crs) for j in range(num_y)] for i in range(num_x)]

    def get_transform_vector(self, resx, resy):
        """ Given resolution it returns a transformation vector

        :param resx: Resolution in x direction
        :type resx: float or int
        :param resy: Resolution in y direction
        :type resy: float or int
        :return: A tuple with 6 numbers representing transformation vector
        :rtype: tuple(float)
        """
        return self.min_x, self._parse_resolution(resx), 0, self.max_y, 0, -self._parse_resolution(resy)

    @staticmethod
    def _parse_resolution(res):
        """ Helper method for parsing given resolution. It will also try to parse a string into float

        :return: A float value of resolution
        :rtype: float
        """
        if isinstance(res, str):
            return float(res.strip('m'))
        if isinstance(res, (int, float)):
            return float(res)

        raise TypeError(f'Resolution should be a float, got resolution of type {type(res)}')

    @staticmethod
    def _to_tuple(bbox):
        """ Converts the input bbox representation (see the constructor docstring for a list of valid representations)
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
        if isinstance(bbox, shapely.geometry.base.BaseGeometry):
            return bbox.bounds
        raise TypeError('Invalid bbox representation')

    @staticmethod
    def _tuple_from_list_or_tuple(bbox):
        """ Converts a list or tuple representation of a bbox into a flat tuple representation.

        :param bbox: a list or tuple with 4 coordinates that is either flat or nested
        :return: tuple (min_x,min_y,max_x,max_y)
        :raises: TypeError
        """
        if len(bbox) == 4:
            return tuple(map(float, bbox))
        if len(bbox) == 2 and all(isinstance(point, (list, tuple)) for point in bbox):
            return BBox._tuple_from_list_or_tuple(bbox[0] + bbox[1])
        raise TypeError('Expected a valid list or tuple representation of a bbox')

    @staticmethod
    def _tuple_from_str(bbox):
        """ Parses a string of numbers separated by any combination of commas and spaces

        :param bbox: e.g. str of the form `min_x ,min_y  max_x, max_y`
        :return: tuple (min_x,min_y,max_x,max_y)
        """
        return tuple(float(s) for s in bbox.replace(',', ' ').split() if s)

    @staticmethod
    def _tuple_from_dict(bbox):
        """ Converts a dictionary representation of a bbox into a flat tuple representation

        :param bbox: a dict with keys "min_x, "min_y", "max_x", and "max_y"
        :return: tuple (min_x,min_y,max_x,max_y)
        :raises: KeyError
        """
        return bbox['min_x'], bbox['min_y'], bbox['max_x'], bbox['max_y']

    @staticmethod
    def _tuple_from_bbox(bbox):
        """ Converts a BBox instance into a tuple

        :param bbox: An instance of the BBox type
        :return: tuple (min_x, min_y, max_x, max_y)
        """
        return bbox.lower_left + bbox.upper_right


class Geometry(BaseGeometry):
    """ A class that combines shapely geometry with coordinate reference system. It currently supports polygons and
    multipolygons.

    It can be initialize with any of the following geometry representations:
    - `shapely.geometry.Polygon` or `shapely.geometry.MultiPolygon`
    - A GeoJSON dictionary with (multi)polygon coordinates
    - A WKT string with (multi)polygon coordinates
    """
    def __init__(self, geometry, crs):
        """
        :param geometry: A polygon or multipolygon in any valid representation
        :type geometry: shapely.geometry.Polygon or shapely.geometry.MultiPolygon or dict or str
        :param crs: Coordinate reference system of the geometry
        :type crs: constants.CRS
        """
        self._geometry = self._parse_geometry(geometry)

        super().__init__(crs)

    def __repr__(self):
        """ Method for class representation
        """
        return f'{self.__class__.__name__}({self.wkt}, crs={repr(self.crs)})'

    def __eq__(self, other):
        """ Method for comparing two Geometry classes

        :param other: Another Geometry object
        :type other: Geometry
        :return: `True` if geometry objects have the same geometry and CRS and `False` otherwise
        :rtype: bool
        """
        if not isinstance(other, Geometry):
            return False
        return self.geometry == other.geometry and self.crs is other.crs

    def reverse(self):
        """ Returns a new Geometry object where x and y coordinates are switched

        :return: New Geometry object with switched coordinates
        :rtype: Geometry
        """
        return Geometry(shapely.ops.transform(lambda x, y: (y, x), self.geometry), crs=self.crs)

    def transform(self, crs, always_xy=True):
        """ Transforms Geometry from current CRS to target CRS

        :param crs: target CRS
        :type crs: constants.CRS
        :param always_xy: Parameter that is passed to `pyproj.Transformer` object and defines axis order for
            transformation. The default value `True` is in most cases the correct one.
        :type always_xy: bool
        :return: Geometry in target CRS
        :rtype: Geometry
        """
        new_crs = CRS(crs)

        geometry = self.geometry
        if new_crs is not self.crs:
            transform_function = self.crs.get_transform_function(new_crs, always_xy=always_xy)
            geometry = shapely.ops.transform(transform_function, geometry)

        return Geometry(geometry, crs=new_crs)

    @classmethod
    def from_geojson(cls, geojson, crs=None):
        """ Create Geometry object from geojson. It will parse crs from geojson (if info is available),
        otherwise it will be set to crs (WGS84 if parameter is empty)

        :param geojson: geojson geometry (single feature)
        :param crs: crs to be used if not available in geojson, CRS.WGS84 if not provided
        :return: Geometry object
        """
        try:
            crs = CRS(geojson['crs']['properties']['name'])
        except (KeyError, AttributeError, TypeError):
            pass

        if not crs:
            crs = CRS.WGS84

        return cls(geojson, crs=crs)

    @property
    def geometry(self):
        """ Returns shapely object representing geometry in this class

        :return: A polygon or a multipolygon in shapely format
        :rtype: shapely.geometry.Polygon or shapely.geometry.MultiPolygon
        """
        return self._geometry

    @property
    def bbox(self):
        """ Returns BBox object representing bounding box around the geometry

        :return: A bounding box, with same CRS
        :rtype: BBox
        """
        return BBox(self.geometry, self.crs)

    @staticmethod
    def _parse_geometry(geometry):
        """ Parses given geometry into shapely object

        :param geometry:
        :return: Shapely polygon or multipolygon
        :rtype: shapely.geometry.Polygon or shapely.geometry.MultiPolygon
        :raises TypeError
        """
        if isinstance(geometry, str):
            geometry = shapely.wkt.loads(geometry)
        elif isinstance(geometry, dict):
            geometry = shapely.geometry.shape(geometry)
        elif not isinstance(geometry, shapely.geometry.base.BaseGeometry):
            raise TypeError('Unsupported geometry representation')

        if not isinstance(geometry, (shapely.geometry.Polygon, shapely.geometry.MultiPolygon)):
            raise ValueError(f'Supported geometry types are polygon and multipolygon, got {type(geometry)}')

        return geometry


class BBoxCollection(BaseGeometry):
    """ A collection of bounding boxes
    """
    def __init__(self, bbox_list):
        """
        :param bbox_list: A list of BBox objects which have to be in the same CRS
        :type bbox_list: list(BBox)
        """
        self._bbox_list, crs = self._parse_bbox_list(bbox_list)
        self._geometry = self._get_geometry()

        super().__init__(crs)

    def __repr__(self):
        """ Method for class representation
        """
        bbox_list_repr = ', '.join([repr(bbox) for bbox in self.bbox_list])
        return f'{self.__class__.__name__}({bbox_list_repr})'

    def __eq__(self, other):
        """ Method for comparing two BBoxCollection classes
        """
        if not isinstance(other, BBoxCollection):
            return False
        return self.crs is other.crs and len(self.bbox_list) == len(other.bbox_list) and \
            all(bbox == other_bbox for bbox, other_bbox in zip(self, other))

    def __iter__(self):
        """ This method enables iteration over bounding boxes in collection
        """
        return iter(self.bbox_list)

    @property
    def bbox_list(self):
        """ Returns the list of bounding boxes from collection

        :return: The list of bounding boxes
        :rtype: list(BBox)
        """
        return self._bbox_list

    @property
    def geometry(self):
        """ Returns shapely object representing geometry

        :return: A multipolygon of bounding boxes
        :rtype: shapely.geometry.MultiPolygon
        """
        return self._geometry

    @property
    def bbox(self):
        """ Returns BBox object representing bounding box around the geometry

        :return: A bounding box, with same CRS
        :rtype: BBox
        """
        return BBox(self.geometry, self.crs)

    def reverse(self):
        """ Returns a new BBoxCollection object where all x and y coordinates are switched

        :return: New Geometry object with switched coordinates
        :rtype: BBoxCollection
        """
        return BBoxCollection([bbox.reverse() for bbox in self.bbox_list])

    def transform(self, crs, always_xy=True):
        """ Transforms BBoxCollection from current CRS to target CRS

        :param crs: target CRS
        :type crs: constants.CRS
        :param always_xy: Parameter that is passed to `pyproj.Transformer` object and defines axis order for
            transformation. The default value `True` is in most cases the correct one.
        :type always_xy: bool
        :return: BBoxCollection in target CRS
        :rtype: BBoxCollection
        """
        return BBoxCollection([bbox.transform(crs, always_xy=always_xy) for bbox in self.bbox_list])

    def _get_geometry(self):
        """ Creates a multipolygon of bounding box polygons
        """
        return shapely.geometry.MultiPolygon([bbox.geometry for bbox in self.bbox_list])

    @staticmethod
    def _parse_bbox_list(bbox_list):
        """ Helper method for parsing a list of bounding boxes
        """
        if isinstance(bbox_list, BBoxCollection):
            return bbox_list.bbox_list, bbox_list.crs

        if not isinstance(bbox_list, list) or not bbox_list:
            raise ValueError('Expected non-empty list of BBox objects')

        for bbox in bbox_list:
            if not isinstance(bbox, BBox):
                raise ValueError(f'Elements in the list should be of type {BBox.__name__}, got {type(bbox)}')

        crs = bbox_list[0].crs
        for bbox in bbox_list:
            if bbox.crs is not crs:
                raise ValueError('All bounding boxes should have the same CRS')

        return bbox_list, crs

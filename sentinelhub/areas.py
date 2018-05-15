"""
Module for working with large geographical areas
"""
import itertools
from abc import ABC, abstractmethod

from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import cascaded_union

from .common import BBox
from .constants import CRS, DataSource
from .geo_utils import transform_point, transform_bbox
from .ogc import WebFeatureService


class AreaSplitter(ABC):
    """
    Abstract class for splitter classes. It implements common methods used for splitting large area into smaller parts.

    :param shape_list: A list of geometrical shapes describing the area of interest
    :type shape_list: list(shapely.geometry.multipolygon.MultiPolygon or shapely.geometry.polygon.Polygon)
    :param crs: Coordinate reference system of the shapes in `shape_list`
    :type crs: sentinelhub.constants.CRS
    :param reduce_bbox_sizes: If True it will reduce the sizes of bounding boxes so that they will tightly fit the given
           geometry in `shape_list`.
    :type reduce_bbox_sizes: bool
    """
    def __init__(self, shape_list, crs, reduce_bbox_sizes=False):
        self._check_shape_list(shape_list)
        self.shape_list = shape_list
        self.area_shape = self._join_shape_list(shape_list)
        self.crs = crs
        self.reduce_bbox_sizes = reduce_bbox_sizes

        self.area_bbox = self.get_area_bbox()
        self.bbox_list = None
        self.info_list = None

    @staticmethod
    def _check_shape_list(shape_list):
        """ Checks if the given list of shapes is in correct format

        :param shape_list: The parameter `shape_list` from class initialization
        :type shape_list: list(shapely.geometry.multipolygon.MultiPolygon or shapely.geometry.polygon.Polygon)
        :raises: ValueError
        """
        if not isinstance(shape_list, list):
            raise ValueError('Splitter must be initialized with a list of shapes')
        for shape in shape_list:
            if not isinstance(shape, (Polygon, MultiPolygon)):
                raise ValueError('The list of shapes must contain shapes of type {} or {}'.format(type(Polygon),
                                                                                                  type(MultiPolygon)))

    @staticmethod
    def _join_shape_list(shape_list):
        """Joins a list of shapes together into one shape

        :param shape_list: A list of geometrical shapes describing the area of interest
        :type shape_list: list(shapely.geometry.multipolygon.MultiPolygon or shapely.geometry.polygon.Polygon)
        :return: A multipolygon which is a union of shapes in given list
        :rtype: shapely.geometry.multipolygon.MultiPolygon
        """
        return cascaded_union(shape_list)

    @abstractmethod
    def _make_split(self):
        """The abstract method where the splitting will happen
        """
        raise NotImplementedError

    def get_bbox_list(self, crs=None):
        """Returns a list of bounding boxes that are the result of the split

        :param crs: Coordinate reference system in which the bounding boxes should be returned. If None the CRS will
                    be the default CRS of the splitter.
        :type crs: sentinelhub.constants.CRS or None
        :return: List of bounding boxes
        :rtype: list(sentinelhub.common.BBox)
        """
        if crs:
            return [transform_bbox(bbox, crs) for bbox in self.bbox_list]
        return self.bbox_list

    def get_geometry_list(self):
        """For each bounding box an intersection with the shape of entire given area is calculated. CRS of the returned
        shapes is the same as CRS of the given area.

        :return: List of polygons or multipolygons corresponding to the order of bounding boxes
        :rtype: list(shapely.geometry.multipolygon.MultiPolygon or shapely.geometry.polygon.Polygon)
        """
        return [self._intersection_area(bbox) for bbox in self.bbox_list]

    def get_info_list(self):
        """Returns a list of dictionaries containing information about bounding boxes obtained in split. The order in
        the list matches the order of the list of bounding boxes.

        :return: List of dictionaries
        :rtype: list(sentinelhub.common.BBox)
        """
        return self.info_list

    def get_area_shape(self):
        """Returns a single shape of entire area described with `shape_list` parameter

        :return: A multipolygon which is a union of shapes describing the area
        :rtype: shapely.geometry.multipolygon.MultiPolygon
        """
        return self.area_shape

    def get_area_bbox(self, crs=None):
        """Returns a bounding box of the entire area

        :param crs: Coordinate reference system in which the bounding box should be returned. If None the CRS will
                    be the default CRS of the splitter.
        :type crs: sentinelhub.constants.CRS or None
        :return: A bounding box of the area defined by the `shape_list`
        :rtype: sentinelhub.common.BBox
        """
        bbox_list = [BBox(shape.bounds, crs=self.crs) for shape in self.shape_list]
        area_minx = min([bbox.get_lower_left()[0] for bbox in bbox_list])
        area_miny = min([bbox.get_lower_left()[1] for bbox in bbox_list])
        area_maxx = max([bbox.get_upper_right()[0] for bbox in bbox_list])
        area_maxy = max([bbox.get_upper_right()[1] for bbox in bbox_list])
        bbox = BBox([area_minx, area_miny, area_maxx, area_maxy], crs=self.crs)
        if crs is None:
            return bbox
        return transform_bbox(bbox, crs)

    def _intersects_area(self, bbox):
        """Checks if the bounding box intersects the entire area

        :param bbox: A bounding box
        :type bbox: sentinelhub.common.BBox)
        :return: True if bbox intersects the entire area else False
        :rtype: bool
        """
        return self._bbox_to_area_polygon(bbox).intersects(self.area_shape)

    def _intersection_area(self, bbox):
        """Calculates the intersection of a given bounding box and the entire area

        :param bbox: A bounding box
        :type bbox: sentinelhub.common.BBox)
        :return: A shape of intersection
        :rtype: shapely.geometry.multipolygon.MultiPolygon or shapely.geometry.polygon.Polygon
        """
        return self._bbox_to_area_polygon(bbox).intersection(self.area_shape)

    def _bbox_to_area_polygon(self, bbox):
        """Transforms bounding box into a polygon object in the area CRS.

        :param bbox: A bounding box
        :type bbox: sentinelhub.common.BBox)
        :return: A polygon
        :rtype: shapely.geometry.polygon.Polygon
        """
        projected_bbox = transform_bbox(bbox, self.crs)
        return Polygon(projected_bbox.get_polygon())

    def _reduce_sizes(self):
        """Reduces sizes of bounding boxes
        """
        for i, bbox in enumerate(self.bbox_list):
            bbox_crs = bbox.get_crs()
            self.bbox_list[i] = transform_bbox(BBox(self._intersection_area(bbox).bounds, self.crs), bbox_crs)


class BBoxSplitter(AreaSplitter):
    """A tool that splits the given area into smaller parts. Given the area it calculates its bounding box and splits it
    into smaller bounding boxes of equal size. Then it filters out the bounding boxes that do not intersect the area. If
    specified by user it can also reduce the sizes of the remaining bounding boxes to best fit the area.

    :param shape_list: A list of geometrical shapes describing the area of interest
    :type shape_list: list(shapely.geometry.multipolygon.MultiPolygon or shapely.geometry.polygon.Polygon)
    :param crs: Coordinate reference system of the shapes in `shape_list`
    :type crs: sentinelhub.constants.CRS
    :param split_shape: Parameter that describes the shape in which the area bounding box will be split. It can be a
                        tuple of the form `(n, m)` which means the area bounding box will be split into `n` columns and
                        `m` rows. It can also be a single integer `n` which is the same as `(n, n)`.
    :type split_shape: int or (int, int)
    :param reduce_bbox_sizes: If True it will reduce the sizes of bounding boxes so that they will tightly fit the given
           area geometry from `shape_list`.
    :type reduce_bbox_sizes: bool
    """
    def __init__(self, shape_list, crs, split_shape, **kwargs):
        super(BBoxSplitter, self).__init__(shape_list, crs, **kwargs)

        self.split_shape = self._parse_split_shape(split_shape)

        self._make_split()

    @staticmethod
    def _parse_split_shape(split_shape):
        """Parses the parameter `split_shape`

        :param split_shape: The parameter `split_shape` from class initialization
        :type split_shape: int or (int, int)
        :return: A tuple of n
        :rtype: (int, int)
        :raises: ValueError
        """
        if isinstance(split_shape, int):
            return split_shape, split_shape
        if isinstance(split_shape, (tuple, list)):
            if len(split_shape) == 2 and isinstance(split_shape[0], int) and isinstance(split_shape[1], int):
                return split_shape[0], split_shape[1]
            raise ValueError("Content of split_shape {} must be 2 integers.".format(split_shape))
        raise ValueError("Split shape must be an int or a tuple of 2 integers.")

    def _make_split(self):
        """This method makes the split
        """
        columns, rows = self.split_shape
        bbox_partition = self.area_bbox.get_partition(columns, rows)

        self.bbox_list = []
        self.info_list = []
        for i, j in itertools.product(range(columns), range(rows)):
            if self._intersects_area(bbox_partition[i][j]):
                self.bbox_list.append(bbox_partition[i][j])

                info = {'parent_bbox': self.area_bbox,
                        'index_x': i,
                        'index_y': j}
                self.info_list.append(info)

        if self.reduce_bbox_sizes:
            self._reduce_sizes()


class OsmSplitter(AreaSplitter):
    """A tool that splits the given area into smaller parts. For the splitting it uses Open Street Map (OSM) grid on the
    specified zoom level. It calculates bounding boxes of all OSM tiles that intersect the area. If specified by user
    it can also reduce the sizes of the remaining bounding boxes to best fit the area.

    :param shape_list: A list of geometrical shapes describing the area of interest
    :type shape_list: list(shapely.geometry.multipolygon.MultiPolygon or shapely.geometry.polygon.Polygon)
    :param crs: Coordinate reference system of the shapes in `shape_list`
    :type crs: sentinelhub.constants.CRS
    :param zoom_level: A zoom level defined by OSM. Level 0 is entire world, level 1 splits the world into 4 parts, etc.
    :type zoom_level: int
    :param reduce_bbox_sizes: If True it will reduce the sizes of bounding boxes so that they will tightly fit the given
           area geometry from `shape_list`.
    :type reduce_bbox_sizes: bool
    """
    POP_WEB_MAX = transform_point((180, 0), CRS.WGS84, CRS.POP_WEB)[0]

    def __init__(self, shape_list, crs, zoom_level, **kwargs):
        super(OsmSplitter, self).__init__(shape_list, crs, **kwargs)

        self.zoom_level = zoom_level

        self._make_split()

    def _make_split(self, ):
        """This method makes the split
        """
        self.area_bbox = self.get_area_bbox(CRS.POP_WEB)
        self._check_area_bbox()

        self.bbox_list = []
        self.info_list = []
        self._recursive_split(self.get_world_bbox(), 0, 0, 0)

        for i, bbox in enumerate(self.bbox_list):
            self.bbox_list[i] = transform_bbox(bbox, self.crs)

        if self.reduce_bbox_sizes:
            self._reduce_sizes()

    def _check_area_bbox(self):
        """The method checks if the area bounding box is completely inside the OSM grid. That means that its latitudes
        must be contained in the interval (-85.0511, 85.0511)

        :raises: ValueError
        """
        for coord in self.area_bbox:
            if abs(coord) > self.POP_WEB_MAX:
                raise ValueError('OsmTileSplitter only works for areas which have latitude in interval '
                                 '(-85.0511, 85.0511)')

    def get_world_bbox(self):
        """Creates a bounding box of the entire world in EPSG: 3857
        :return: Bounding box of entire world
        :rtype: sentinelhub.common.BBox
        """
        return BBox((-self.POP_WEB_MAX, -self.POP_WEB_MAX, self.POP_WEB_MAX, self.POP_WEB_MAX), crs=CRS.POP_WEB)

    def _recursive_split(self, bbox, zoom_level, column, row):
        """Method that recursively creates bounding boxes of OSM grid that intersect the area.

        :param bbox: Bounding box
        :type bbox: sentinelhub.common.BBox
        :param zoom_level: OSM zoom level
        :type zoom_level: int
        :param column: Column in the OSM grid
        :type column: int
        :param row: Row in the OSM grid
        :type row: int
        """
        if zoom_level == self.zoom_level:
            self.bbox_list.append(bbox)
            self.info_list.append({'zoom_level': zoom_level,
                                   'index_x': column,
                                   'index_y': row})
            return

        bbox_partition = bbox.get_partition(2, 2)
        for i, j in itertools.product(range(2), range(2)):
            if self._intersects_area(bbox_partition[i][j]):
                self._recursive_split(bbox_partition[i][j], zoom_level + 1, 2 * column + i, 2 * row + 1 - j)


class TileSplitter(AreaSplitter):
    """A tool that splits the given area into smaller parts. Given the area, time interval and data source it collects
    info from Sentinel Hub WFS service about all satellite tiles intersecting the area. For each of them it calculates
    bounding box and if specified it splits these bounding boxes into smaller bounding boxes. Then it filters out the
    ones that do not intersect the area. If specified by user it can also reduce the sizes of the remaining bounding
    boxes to best fit the area.

    :param shape_list: A list of geometrical shapes describing the area of interest
    :type shape_list: list(shapely.geometry.multipolygon.MultiPolygon or shapely.geometry.polygon.Polygon)
    :param crs: Coordinate reference system of the shapes in `shape_list`
    :type crs: sentinelhub.constants.CRS
    :param time_interval: Interval with start and end date of the form YYYY-MM-DDThh:mm:ss or YYYY-MM-DD
    :type time_interval: (str, str)
    :param tile_split_shape: Parameter that describes the shape in which the satellite tile bounding boxes will be
                             split. It can be a tuple of the form `(n, m)` which means the tile bounding boxes will be
                             split into `n` columns and `m` rows. It can also be a single integer `n` which is the same
                             as `(n, n)`.
    :type split_shape: int or (int, int)
    :param data_source: Source of requested satellite data. Default is Sentinel-2 L1C data.
    :type data_source: sentinelhub.constants.DataSource
    :param instance_id: User's Sentinel Hub instance id. If ``None`` the instance id is taken from the ``config.json``
                        configuration file.
    :type instance_id: str
    :param reduce_bbox_sizes: If True it will reduce the sizes of bounding boxes so that they will tightly fit the given
           area geometry from `shape_list`.
    :type reduce_bbox_sizes: bool
    """
    def __init__(self, shape_list, crs, time_interval, tile_split_shape=1, data_source=DataSource.SENTINEL2_L1C,
                 instance_id=None, **kwargs):
        super(TileSplitter, self).__init__(shape_list, crs, **kwargs)

        if data_source is DataSource.DEM:
            raise ValueError('This splitter does not support splitting area by DEM tiles. Please specify some other '
                             'DataSource')

        self.time_interval = time_interval
        self.tile_split_shape = tile_split_shape
        self.data_source = data_source
        self.instance_id = instance_id

        self.tile_dict = None

        self._make_split()

    def _make_split(self):
        """This method makes the split
        """
        self.tile_dict = {}

        wfs = WebFeatureService(self.area_bbox, self.time_interval, data_source=self.data_source,
                                instance_id=self.instance_id)
        date_list = wfs.get_dates()
        geometry_list = wfs.get_geometries()
        for tile_info, (date, geometry) in zip(wfs, zip(date_list, geometry_list)):
            tile_name = ''.join(tile_info['properties']['path'].split('/')[4:7])
            if tile_name not in self.tile_dict:
                self.tile_dict[tile_name] = {'bbox': BBox(tile_info['properties']['mbr'],
                                                          crs=tile_info['properties']['crs']),
                                             'times': [],
                                             'geometries': []}
            self.tile_dict[tile_name]['times'].append(date)
            self.tile_dict[tile_name]['geometries'].append(geometry)

        self.tile_dict = {tile_name: tile_props for tile_name, tile_props in self.tile_dict.items() if
                          self._intersects_area(tile_props['bbox'])}

        self.bbox_list = []
        self.info_list = []

        for tile_name, tile_info in self.tile_dict.items():
            tile_bbox = tile_info['bbox']
            bbox_splitter = BBoxSplitter([Polygon(tile_bbox.get_polygon())], tile_bbox.get_crs(),
                                         split_shape=self.tile_split_shape, reduce_bbox_sizes=self.reduce_bbox_sizes)

            for bbox, info in zip(bbox_splitter.get_bbox_list(), bbox_splitter.get_info_list()):
                if self._intersects_area(bbox):
                    info['tile'] = tile_name

                    self.bbox_list.append(bbox)
                    self.info_list.append(info)

        if self.reduce_bbox_sizes:
            self._reduce_sizes()

    def get_tile_dict(self):
        """Returns the dictionary of satellite tiles intersecting the area geometry. For each tile they contain info
        about their bounding box and lists of acquisitions and geometries

        :return: Dictionary containing info about tiles intersecting the area
        :rtype: dict
        """
        return self.tile_dict

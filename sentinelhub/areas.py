"""
Module for working with large geographical areas
"""
from abc import ABC, abstractmethod

from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import cascaded_union

from .common import BBox
from .constants import CRS, DataSource
from .geo_utils import transform_point, transform_bbox
from .ogc import WebFeatureService


class AreaSplitter(ABC):

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
        if not isinstance(shape_list, list):
            raise ValueError('Splitter must be initialized with a list of shapes')
        for shape in shape_list:
            if not isinstance(shape, (Polygon, MultiPolygon)):
                raise ValueError('The list of shapes must contain shapes of type {} or {}'.format(type(Polygon),
                                                                                                  type(MultiPolygon)))

    @staticmethod
    def _join_shape_list(shape_list):
        return cascaded_union(shape_list)

    @abstractmethod
    def _make_split(self):
        raise NotImplementedError

    def get_bbox_list(self, crs=None):
        if crs:
            return [transform_bbox(bbox, crs) for bbox in self.bbox_list]
        return self.bbox_list

    def get_info_list(self):
        return self.info_list

    def get_area_shape(self):
        return self.area_shape

    def get_area_bbox(self, crs=None):
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
        return self._bbox_to_area_polygon(bbox).intersects(self.area_shape)

    def _intersection_area(self, bbox):
        return self._bbox_to_area_polygon(bbox).intersection(self.area_shape)

    def _bbox_to_area_polygon(self, bbox):
        projected_bbox = transform_bbox(bbox, self.crs)
        return Polygon(projected_bbox.get_polygon())

    def _reduce_sizes(self):
        for i, bbox in enumerate(self.bbox_list):
            self.bbox_list[i] = BBox(self._intersection_area(bbox).bounds, self.crs)


class BBoxSplitter(AreaSplitter):

    def __init__(self, shape_list, crs, split_shape, **kwargs):
        super(BBoxSplitter, self).__init__(shape_list, crs, **kwargs)

        self.split_shape = self._parse_split_shape(split_shape)

        self._make_split()

    @staticmethod
    def _parse_split_shape(split_shape):
        if isinstance(split_shape, int):
            return split_shape, split_shape
        if isinstance(split_shape, (tuple, list)):
            if len(split_shape) == 2 and isinstance(split_shape[0], int) and isinstance(split_shape[1], int):
                return split_shape[0], split_shape[1]
            raise ValueError("Content of split_shape {} must be 2 integers.".format(split_shape))
        raise ValueError("Split shape must be an int or a tuple of 2 integers.")

    def _make_split(self):
        columns, rows = self.split_shape
        bbox_partition = self.area_bbox.get_partition(columns, rows)

        self.bbox_list = []
        self.info_list = []
        for i in range(columns):
            for j in range(rows):
                if self._intersects_area(bbox_partition[i][j]):
                    self.bbox_list.append(bbox_partition[i][j])

                    info = {'parent_bbox': self.area_bbox,
                            'index_x': i,
                            'index_y': j}
                    self.info_list.append(info)

        if self.reduce_bbox_sizes:
            self._reduce_sizes()


class OsmSplitter(AreaSplitter):

    POP_WEB_MAX = transform_point((180, 0), CRS.WGS84, CRS.POP_WEB)[0]

    def __init__(self, shape_list, crs, zoom_level, **kwargs):
        super(OsmSplitter, self).__init__(shape_list, crs, **kwargs)

        self.zoom_level = zoom_level

        self._make_split()

    def _make_split(self, ):
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
        for coord in self.area_bbox:
            if abs(coord) > self.POP_WEB_MAX:
                raise ValueError('OsmTileSplitter only works for areas which have latitude in interval '
                                 '(-85.0511, 85.0511)')

    def get_world_bbox(self):
        return BBox((-self.POP_WEB_MAX, -self.POP_WEB_MAX, self.POP_WEB_MAX, self.POP_WEB_MAX), crs=CRS.POP_WEB)

    def _recursive_split(self, bbox, zoom_level, row, column):
        if zoom_level == self.zoom_level:
            self.bbox_list.append(bbox)
            self.info_list.append({'zoom_level': zoom_level,
                                   'index_x': row,
                                   'index_y': column})
            return

        bbox_partition = bbox.get_partition(2, 2)
        for i in range(2):
            for j in range(2):
                if self._intersects_area(bbox_partition[i][j]):
                    self._recursive_split(bbox_partition[i][j], zoom_level + 1, 2 * row + i, 2 * column + 1 - j)


class TileSplitter(AreaSplitter):

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

    def get_tile_dict(self):
        return self.tile_dict


class AreaMerger:

    def __init__(self):
        pass

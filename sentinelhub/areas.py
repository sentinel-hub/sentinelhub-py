"""
Module for working with large geographical areas
"""

from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import cascaded_union

from .common import BBox


class AreaSplitter:

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

    def get_bbox_list(self):
        return self.bbox_list

    def get_info_list(self):
        return self.info_list

    def get_area_shape(self):
        return self.area_shape

    def get_area_bbox(self):
        bbox_list = [BBox(shape.bounds, crs=self.crs) for shape in self.shape_list]
        area_minx = min([bbox.get_lower_left()[0] for bbox in bbox_list])
        area_miny = min([bbox.get_lower_left()[1] for bbox in bbox_list])
        area_maxx = max([bbox.get_upper_right()[0] for bbox in bbox_list])
        area_maxy = max([bbox.get_upper_right()[1] for bbox in bbox_list])
        return BBox([area_minx, area_miny, area_maxx, area_maxy], crs=self.crs)

    def _intersects_area(self, bbox):
        polygon = Polygon(bbox.get_polygon())
        return polygon.intersects(self.area_shape)

    def _reduce_sizes(self):
        for i, bbox in enumerate(self.bbox_list):
            bbox_polygon = Polygon(bbox.get_polygon())
            bbox_polygon = bbox_polygon.intersection(self.area_shape)

            self.bbox_list[i] = BBox(bbox_polygon.bounds, self.crs)


class SentinelTileSplitter(AreaSplitter):
    pass
    # TODO
    # https://fusiontables.google.com/data?docid=17LBHEZDi8Mpu_OyNa_1zTGrwAxVf406RtpkzSxYD#map:id=3


class OsmTileSplitter(AreaSplitter):

    def __init__(self, shape_list, crs, zoom_level, **kwargs):
        super(OsmTileSplitter, self).__init__(shape_list, crs, **kwargs)

        self.zoom_level = zoom_level

        self._make_split()

    def _make_split(self, ):
        # TODO
        raise NotImplementedError


class BBoxSplitter(AreaSplitter):

    def __init__(self, shape_list, crs, split_shape, **kwargs):
        super(BBoxSplitter, self).__init__(shape_list, crs, **kwargs)

        self.split_shape = self._parse_split_shape(split_shape)

        self._make_split()

    @staticmethod
    def _parse_split_shape(split_shape):
        if isinstance(split_shape, int):
            return split_shape, split_shape
        if isinstance(split_shape, tuple) or isinstance(split_shape, list):
            if len(split_shape) == 2 and isinstance(split_shape[0], int) and isinstance(split_shape[1], int):
                return split_shape[0], split_shape[1]
            raise ValueError("Content of split_shape {} must be 2 integers.".format(split_shape))
        raise ValueError("Split shape must be an int or a tuple of 2 integers.")

    def _make_split(self):
        nx, ny = self.split_shape
        bbox_partition = self.area_bbox.get_partition(nx, ny)

        self.bbox_list = []
        self.info_list = []
        for i in range(nx):
            for j in range(ny):
                if self._intersects_area(bbox_partition[i][j]):
                    self.bbox_list.append(bbox_partition[i][j])

                    info = {'parent_bbox': self.area_bbox,
                            'index_x': i,
                            'index_y': j}
                    self.info_list.append(info)

        if self.reduce_bbox_sizes:
            self._reduce_sizes()

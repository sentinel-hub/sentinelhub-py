import os
import itertools

import pytest
import shapely.geometry

from sentinelhub import (
    BBoxSplitter, OsmSplitter, TileSplitter, CustomGridSplitter, BBox, read_data, CRS, DataCollection, UtmGridSplitter,
    UtmZoneSplitter
)
from sentinelhub.testing_utils import get_input_folder

geojson = read_data(os.path.join(get_input_folder(__file__), 'cies_islands.json'))
AREA = shapely.geometry.shape(geojson)
BBOX_GRID = [BBox((x / 10, y / 100, (x + 1) / 10, (y + 1) / 100), CRS.WGS84)
             for x, y in itertools.product(range(-90, -87), range(4200, 4250))]


@pytest.mark.parametrize('constructor, args, kwargs, bbox_len', [
    [BBoxSplitter, ([AREA], CRS.WGS84, 5), dict(reduce_bbox_sizes=True), 19],
    [OsmSplitter, ([AREA], CRS.WGS84, 15), dict(reduce_bbox_sizes=True), 24],
    [CustomGridSplitter, ([AREA], CRS.WGS84, BBOX_GRID), dict(bbox_split_shape=(3, 4), reduce_bbox_sizes=False), 41],
    [UtmGridSplitter, ([AREA], CRS.WGS84), dict(bbox_size=(1200, 1200)), 16],
    [UtmZoneSplitter, ([AREA], CRS.WGS84), dict(bbox_size=(1000, 1000)), 19],
    [UtmZoneSplitter, ([AREA], CRS.WGS84), dict(bbox_size=(1000, 1000), offset=(500, 500)), 21],
    pytest.param(
        TileSplitter, ([AREA], CRS.WGS84, ('2017-10-01', '2018-03-01')),
        dict(tile_split_shape=40, data_collection=DataCollection.SENTINEL2_L1C, reduce_bbox_sizes=True), 13,
        marks=pytest.mark.sh_integration
    ),
])
def test_return_type(constructor, args, kwargs, bbox_len):
    splitter = constructor(*args, **kwargs)
    return_lists = [
        (splitter.get_bbox_list(buffer=0.2), BBox),
        (splitter.get_info_list(), dict),
        (splitter.get_geometry_list(), (shapely.geometry.Polygon, shapely.geometry.MultiPolygon))
    ]

    for return_list, item_type in return_lists:
        assert isinstance(return_list, list)
        assert len(return_list) == bbox_len
        for return_item in return_list:
            assert isinstance(return_item, item_type)

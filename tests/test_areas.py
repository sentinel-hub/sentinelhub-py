from __future__ import annotations

import itertools
import os
from typing import Any

import pytest
import shapely.geometry

from sentinelhub import (
    CRS,
    BBox,
    BBoxSplitter,
    CustomGridSplitter,
    DataCollection,
    Geometry,
    OsmSplitter,
    TileSplitter,
    UtmGridSplitter,
    UtmZoneSplitter,
    read_data,
)
from sentinelhub.areas import AreaSplitter
from sentinelhub.testing_utils import get_input_folder

geojson = read_data(os.path.join(get_input_folder(__file__), "cies_islands.json"))
AREA = shapely.geometry.shape(geojson)
REPROJECTED_AREA = Geometry(AREA, crs=CRS.WGS84).transform(CRS("32629"))
BBOX_GRID = [
    BBox((x / 10, y / 100, (x + 1) / 10, (y + 1) / 100), CRS.WGS84)
    for x, y in itertools.product(range(-90, -87), range(4200, 4250))
]


@pytest.mark.parametrize(
    ("constructor", "args", "kwargs", "bbox_len"),
    [
        (BBoxSplitter, ([AREA], CRS.WGS84, 5), dict(reduce_bbox_sizes=True), 19),
        (OsmSplitter, ([AREA], CRS.WGS84, 15), dict(reduce_bbox_sizes=True), 24),
        (
            CustomGridSplitter,
            ([AREA], CRS.WGS84, BBOX_GRID),
            dict(bbox_split_shape=(3, 4), reduce_bbox_sizes=False),
            41,
        ),
        (UtmGridSplitter, ([AREA], CRS.WGS84), dict(bbox_size=(1200, 1200)), 16),
        (UtmZoneSplitter, ([AREA], CRS.WGS84), dict(bbox_size=(1000, 1000)), 19),
        (UtmZoneSplitter, ([AREA], CRS.WGS84), dict(bbox_size=(1000, 1000), offset=(500, 500)), 21),
        (UtmZoneSplitter, ([shapely.box(0, 0, 1, 1)], CRS.WGS84), dict(bbox_size=(10000, 10000)), 144),
        pytest.param(
            TileSplitter,
            ([AREA], CRS.WGS84, ("2017-10-01", "2018-03-01")),
            dict(tile_split_shape=40, data_collection=DataCollection.SENTINEL2_L1C, reduce_bbox_sizes=True),
            13,
            marks=pytest.mark.sh_integration,
        ),
        pytest.param(
            TileSplitter,
            ([AREA], CRS.WGS84, ("2020-10-01", "2020-10-05")),
            dict(tile_split_shape=10, data_collection=DataCollection.LANDSAT_OT_L2, reduce_bbox_sizes=True),
            3,
            marks=pytest.mark.sh_integration,
        ),
    ],
)
def test_return_type(constructor: type[AreaSplitter], args: list, kwargs: dict[str, Any], bbox_len: int) -> None:
    splitter = constructor(*args, **kwargs)

    return_lists: list[tuple[list, type | tuple[type, ...]]] = [
        (splitter.get_bbox_list(buffer=0.2), BBox),
        (splitter.get_info_list(), dict),
        (splitter.get_geometry_list(), (shapely.geometry.Polygon, shapely.geometry.MultiPolygon)),
    ]
    for return_list, item_type in return_lists:
        assert isinstance(return_list, list)
        assert len(return_list) == bbox_len
        for return_item in return_list:
            assert isinstance(return_item, item_type)


@pytest.mark.parametrize(
    ("args", "kwargs", "bbox_len"),
    [
        (([REPROJECTED_AREA], CRS("32629")), dict(split_size=(2000, 4000), reduce_bbox_sizes=False), 4),
        (([REPROJECTED_AREA], CRS("32629")), dict(split_size=(1000, 2000), reduce_bbox_sizes=True), 11),
        (([AREA], CRS.WGS84), dict(split_size=1000, reduce_bbox_sizes=True), 1),
        (([AREA], CRS("32629")), dict(split_size=1000, reduce_bbox_sizes=True), 1),
    ],
)
def test_bbox_splitter_by_size(args: list, kwargs: dict[str, Any], bbox_len: int) -> None:
    splitter = BBoxSplitter(*args, **kwargs)
    assert len(splitter.get_geometry_list()) == bbox_len
    assert all(splitter.crs == bbox.crs for bbox in splitter.get_bbox_list())

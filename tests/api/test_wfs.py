"""
Test for Sentinel Hub WFS
"""
from __future__ import annotations

import datetime
from typing import Any

import pytest
from shapely.geometry import MultiPolygon

from sentinelhub import CRS, BBox, DataCollection, WebFeatureService

pytestmark = pytest.mark.sh_integration


@pytest.mark.parametrize(
    ("args", "kwargs", "expected_len"),
    [
        (
            [
                BBox(bbox=(-5.23, 48.0, -5.03, 48.17), crs=CRS.WGS84),
                (datetime.date(year=2017, month=1, day=5), datetime.date(year=2017, month=12, day=16)),
            ],
            dict(data_collection=DataCollection.SENTINEL2_L1C, maxcc=0.1),
            13,
        ),
        (
            [BBox(bbox=(-5.23, 48.0, -5.03, 48.17), crs=CRS.WGS84), "latest"],
            dict(data_collection=DataCollection.SENTINEL2_L2A),
            1,
        ),
    ],
)
def test_wfs(args: list, kwargs: dict[str, Any], expected_len: int) -> None:
    iterator = WebFeatureService(*args, **kwargs)
    features = list(iterator)
    dates = iterator.get_dates()
    geometries = iterator.get_geometries()
    tiles = iterator.get_tiles()

    for result_list, expected_type in [
        (features, dict),
        (dates, datetime.datetime),
        (geometries, MultiPolygon),
        (tiles, tuple),
    ]:
        assert len(result_list) == expected_len
        for result in result_list:
            assert isinstance(result, expected_type)

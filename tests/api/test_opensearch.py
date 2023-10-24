"""
Tests for Sentinel Hub Opensearch service interface
"""

from sentinelhub import CRS, BBox, get_area_dates, get_tile_info


def test_get_tile_info() -> None:
    tile_info = get_tile_info("T17SNV", "2015-11-29", aws_index=1)
    assert isinstance(tile_info, dict)


def test_get_area_dates() -> None:
    bbox = BBox([1059111.463919402, 4732980.791418114, 1061557.4488245277, 4735426.776323237], crs=CRS.POP_WEB)
    dates = get_area_dates(bbox, ("2016-01-23", "2016-11-24"), maxcc=0.7)
    assert isinstance(dates, list)
    assert len(dates) == 22

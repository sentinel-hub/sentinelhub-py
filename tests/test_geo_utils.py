"""
Test for geo_utils module and correctness of geographical transformations
"""
from __future__ import annotations

import pytest

from sentinelhub import CRS, BBox
from sentinelhub.geo_utils import (
    bbox_to_dimensions,
    bbox_to_resolution,
    get_image_dimension,
    get_utm_crs,
    pixel_to_utm,
    transform_point,
    utm_to_pixel,
)

BBOX_WGS84 = BBox(((111.6388, 8.6488), (111.6988, 8.6868)), CRS.WGS84)
BBOX_UTM = BBox(((570280, 956083), (576884, 960306)), CRS("32649"))
BBOX_POP_WEB = BBox(((12427574, 966457), (12434253, 970736)), CRS.POP_WEB)

BBOX_2 = BBox(((570000, 956000), (571000, 958000)), CRS("32649"))
BBOX_3 = BBox(((100, -10.5), (101, -10)), CRS.WGS84)

GEOREFERENCING_TRANSFORM = (570851.8316965176, 512, 0, 960429.6742984429, 0, -512)


@pytest.mark.parametrize(
    ("wgs84_coordinate", "utm_crs"),
    [
        ((109.988, 9.988), CRS("32649")),
        ((49.889, 49.889), CRS("32639")),
        ((30, -15), CRS("32736")),
    ],
)
def test_get_utm_crs(wgs84_coordinate: tuple[float, float], utm_crs: CRS) -> None:
    assert get_utm_crs(*wgs84_coordinate) is utm_crs


@pytest.mark.parametrize(
    ("input_bbox", "resolution", "expected_dimensions"),
    [
        (BBOX_WGS84, (512, 512), (12.8784, 8.2284)),
        (BBOX_UTM, (512, 50), (12.8984, 84.46)),
        (BBOX_POP_WEB, (50, 512), (131.87, 8.2284)),
        (BBOX_2, (10, 10), (100, 200)),
        (BBOX_3, (500, 500), (219.6, 109.58)),
    ],
)
def test_bbox_to_resolution(
    input_bbox: BBox, resolution: tuple[int, int], expected_dimensions: tuple[float, float]
) -> None:
    assert bbox_to_resolution(input_bbox, *resolution) == pytest.approx(expected_dimensions, rel=1e-4)


@pytest.mark.parametrize(
    ("input_bbox", "resolution", "expected_dimensions"),
    [
        (BBOX_WGS84, 10, (659, 421)),
        (BBOX_UTM, 10, (660, 422)),
        (BBOX_POP_WEB, (20, 50), (330, 84)),
        (BBOX_2, (20, 10), (50, 200)),
        (BBOX_3, (100, 50), (1098, 1096)),
    ],
)
def test_bbox_to_dimensions(
    resolution: float | tuple[float, float], expected_dimensions: tuple[int, int], input_bbox: BBox
) -> None:
    assert bbox_to_dimensions(input_bbox, resolution) == expected_dimensions


@pytest.mark.parametrize(
    ("input_bbox", "height", "width"),
    [
        (BBOX_WGS84, 715, 1119),
        (BBOX_UTM, 715, 1118),
        (BBOX_POP_WEB, 715, 1119),
        (BBOX_2, 10, 5),
        (BBOX_3, 15, 30),
    ],
)
def test_get_image_dimensions(input_bbox: BBox, height: int, width: int) -> None:
    assert get_image_dimension(input_bbox, height=height) == width
    assert get_image_dimension(input_bbox, width=width) == height


@pytest.mark.parametrize("input_bbox", [BBOX_WGS84, BBOX_UTM, BBOX_POP_WEB])
@pytest.mark.parametrize("expected_bbox", [BBOX_WGS84, BBOX_UTM, BBOX_POP_WEB])
def test_bbox_transform(input_bbox: BBox, expected_bbox: BBox) -> None:
    test_bbox = input_bbox.transform(expected_bbox.crs)
    assert tuple(test_bbox) == pytest.approx(tuple(expected_bbox), rel=1e-4)
    assert test_bbox.crs is expected_bbox.crs


@pytest.mark.parametrize(
    ("point", "source_crs", "target_crs", "target_point"),
    [
        ((111.644, 8.655), CRS.WGS84, CRS.POP_WEB, (12428153.23, 967155.41)),
        ((360000.0, 4635040.0), CRS.UTM_31N, CRS.WGS84, (1.313392213, 41.854888581)),
        ((360000.0, 4635040.0), CRS.UTM_31N, CRS.UTM_30N, (858072.82713, 4642667.30545)),
        ((1475000.0, 5100000.0), CRS(2193), CRS.WGS84, (171.43450808, -44.24250942)),
        ((543569.807, 6062625.7678), CRS(3346), CRS.UTM_35N, (350231.496834, 6063682.846723)),
    ],
)
def test_transform_point(
    point: tuple[float, float], source_crs: CRS, target_crs: CRS, target_point: tuple[float, float]
) -> None:
    new_point = transform_point(point, source_crs, target_crs)

    assert new_point == pytest.approx(target_point, rel=1e-8)
    assert transform_point(new_point, target_crs, source_crs) == pytest.approx(point, rel=1e-8)


@pytest.mark.parametrize(
    ("coordinate", "expected_pixel"),
    [
        ((570851, 960429), (0, 0)),
        ((577006, 960429), (0, 12)),
        ((572351, 958770), (3, 3)),
        ((570851, 956770), (7, 0)),
        ((577006, 956770), (7, 12)),
    ],
)
def test_utm_to_pixel(coordinate: tuple[float, float], expected_pixel: tuple[int, int]) -> None:
    assert utm_to_pixel(*coordinate, GEOREFERENCING_TRANSFORM) == expected_pixel


@pytest.mark.parametrize(
    ("pixel", "expected_coordinate"),
    [
        ((0, 0), (570851, 960429)),
        ((0, 12), (576995, 960429)),
        ((3, 3), (572387, 958893)),
        ((7, 0), (570851, 956845)),
        ((7, 12), (576995, 956845)),
    ],
)
def test_pixel_to_utm(pixel: tuple[int, int], expected_coordinate: tuple[float, float]) -> None:
    assert pixel_to_utm(*pixel, GEOREFERENCING_TRANSFORM) == pytest.approx(expected_coordinate, abs=1)

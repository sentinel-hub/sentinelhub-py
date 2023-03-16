"""
Test for geo_utils module and correctness of geographical transformations
"""
from typing import Tuple, Union

import pytest

from sentinelhub import CRS, BBox
from sentinelhub.geo_utils import (
    bbox_to_dimensions,
    bbox_to_resolution,
    get_image_dimension,
    get_utm_crs,
    pixel_to_utm,
    to_wgs84,
    transform_point,
    utm_to_pixel,
    wgs84_to_pixel,
    wgs84_to_utm,
)

BBOX_WGS84 = BBox(((111.644, 8.655), (111.7, 8.688)), CRS.WGS84)
BBOX_UTM = BBox(((570851.8316965176, 956770.2133183606), (577006.4223865959, 960429.6742984429)), CRS("32649"))
BBOX_POP_WEB = BBox(((12428153.230124235, 967155.4076988235), (12434387.12160866, 970871.4287682716)), CRS.POP_WEB)


@pytest.mark.parametrize("input_bbox, expected_bbox", [(BBOX_WGS84, BBOX_UTM), (BBOX_WGS84, BBOX_POP_WEB)])
def test_wgs84_to_utm(input_bbox: BBox, expected_bbox: BBox) -> None:
    point_lower_left = wgs84_to_utm(*input_bbox.lower_left, expected_bbox.crs)
    point_upper_right = wgs84_to_utm(*input_bbox.upper_right, expected_bbox.crs)

    assert point_lower_left == pytest.approx(expected_bbox.lower_left, rel=1e-8)
    assert point_upper_right == pytest.approx(expected_bbox.upper_right, rel=1e-8)


@pytest.mark.parametrize("input_bbox, expected_bbox", [(BBOX_UTM, BBOX_WGS84), (BBOX_POP_WEB, BBOX_WGS84)])
def test_to_wgs84(input_bbox: BBox, expected_bbox: BBox) -> None:
    assert to_wgs84(*input_bbox.lower_left, input_bbox.crs) == pytest.approx(expected_bbox.lower_left, rel=1e-8)
    assert to_wgs84(*input_bbox.upper_right, input_bbox.crs) == pytest.approx(expected_bbox.upper_right, rel=1e-8)


def test_get_utm_crs() -> None:
    assert get_utm_crs(*BBOX_WGS84.lower_left) is BBOX_UTM.crs
    assert get_utm_crs(*BBOX_WGS84.upper_right) is BBOX_UTM.crs


@pytest.mark.parametrize("input_bbox", [BBOX_WGS84, BBOX_UTM, BBOX_POP_WEB])
def test_bbox_to_resolution(input_bbox: BBox) -> None:
    assert bbox_to_resolution(input_bbox, 512, 512) == pytest.approx((12.0207, 7.1474), rel=1e-4)


@pytest.mark.parametrize("input_bbox", [BBOX_WGS84, BBOX_UTM, BBOX_POP_WEB])
@pytest.mark.parametrize("resolution, expected_dimensions", [(10, (615, 366)), ((20, 50), (308, 73))])
def test_bbox_to_dimensions(
    resolution: Union[float, Tuple[float, float]], expected_dimensions: Tuple[int, int], input_bbox: BBox
) -> None:
    assert bbox_to_dimensions(input_bbox, resolution) == expected_dimensions


@pytest.mark.parametrize("input_bbox", [BBOX_WGS84, BBOX_UTM, BBOX_POP_WEB])
def test_get_image_dimensions(input_bbox: BBox) -> None:
    assert get_image_dimension(input_bbox, height=715) == 1203
    assert get_image_dimension(input_bbox, width=1202) == 715


@pytest.mark.parametrize("input_bbox", [BBOX_WGS84, BBOX_UTM, BBOX_POP_WEB])
@pytest.mark.parametrize("expected_bbox", [BBOX_WGS84, BBOX_UTM, BBOX_POP_WEB])
def test_bbox_transform(input_bbox: BBox, expected_bbox: BBox) -> None:
    test_bbox = input_bbox.transform(expected_bbox.crs)
    assert tuple(test_bbox) == pytest.approx(tuple(expected_bbox), rel=1e-8)
    assert test_bbox.crs is expected_bbox.crs


@pytest.mark.parametrize(
    "point, source_crs, target_crs, target_point",
    [
        ((111.644, 8.655), CRS.WGS84, CRS.POP_WEB, (12428153.23, 967155.41)),
        ((360000.0, 4635040.0), CRS.UTM_31N, CRS.WGS84, (1.313392213, 41.854888581)),
        ((360000.0, 4635040.0), CRS.UTM_31N, CRS.UTM_30N, (858072.82713, 4642667.30545)),
        ((1475000.0, 5100000.0), CRS(2193), CRS.WGS84, (171.43450808, -44.24250942)),
        ((543569.807, 6062625.7678), CRS(3346), CRS.UTM_35N, (350231.496834, 6063682.846723)),
    ],
)
def test_transform_point(
    point: Tuple[float, float], source_crs: CRS, target_crs: CRS, target_point: Tuple[float, float]
) -> None:
    new_point = transform_point(point, source_crs, target_crs)

    assert new_point == pytest.approx(target_point, rel=1e-8)
    assert transform_point(new_point, target_crs, source_crs) == pytest.approx(point, rel=1e-8)


GEOREFERENCING_TRANSFORM = (570851.8316965176, 512, 0, 960429.6742984429, 0, -512)


@pytest.mark.parametrize(
    "coordinate, expected_pixel",
    [
        ((111.644, 8.688), (0, 0)),
        ((111.7, 8.688), (0, 12)),
        ((111.66, 8.672), (3, 3)),
        ((111.644, 8.655), (7, 0)),
        ((111.7, 8.655), (7, 12)),
    ],
)
def test_wgs84_to_pixel(coordinate: Tuple[float, float], expected_pixel: Tuple[int, int]) -> None:
    assert wgs84_to_pixel(*coordinate, GEOREFERENCING_TRANSFORM) == expected_pixel


@pytest.mark.parametrize(
    "coordinate, expected_pixel",
    [
        ((570851, 960429), (0, 0)),
        ((577006, 960429), (0, 12)),
        ((572351, 958770), (3, 3)),
        ((570851, 956770), (7, 0)),
        ((577006, 956770), (7, 12)),
    ],
)
def test_utm_to_pixel(coordinate: Tuple[float, float], expected_pixel: Tuple[int, int]) -> None:
    assert utm_to_pixel(*coordinate, GEOREFERENCING_TRANSFORM) == expected_pixel


@pytest.mark.parametrize(
    "pixel, expected_coordinate",
    [
        ((0, 0), (570851, 960429)),
        ((0, 12), (576995, 960429)),
        ((3, 3), (572387, 958893)),
        ((7, 0), (570851, 956845)),
        ((7, 12), (576995, 956845)),
    ],
)
def test_pixel_to_utm(pixel: Tuple[int, int], expected_coordinate: Tuple[float, float]) -> None:
    assert pixel_to_utm(*pixel, GEOREFERENCING_TRANSFORM) == pytest.approx(expected_coordinate, abs=1)

"""
Test for geo_utils module and correctness of geographical transformations
"""
import pytest

from sentinelhub import geo_utils, CRS, BBox


def test_wgs84_to_utm():
    x, y = geo_utils.wgs84_to_utm(15.525078, 44.1440478, CRS.UTM_33N)
    assert (x, y) == pytest.approx((541995.694062, 4888006.132887), rel=1e-8)


def test_to_wgs84():
    lng, lat = geo_utils.to_wgs84(541995.694062, 4888006.132887, CRS.UTM_33N)
    assert (lng, lat) == pytest.approx((15.525078, 44.1440478), rel=1e-8)


def test_get_utm_crs():
    lng, lat = 15.52, 44.14
    crs = geo_utils.get_utm_crs(lng, lat)
    assert crs is CRS.UTM_33N


def test_bbox_to_resolution():
    bbox = BBox(((111.644, 8.655), (111.7, 8.688)), CRS.WGS84)
    resx, resy = geo_utils.bbox_to_resolution(bbox, 512, 512)

    assert (resx, resy) == pytest.approx((12.0207, 7.1474), rel=1e-4)


@pytest.mark.parametrize('resolution, expected_dimensions', [
    (10, (615, 366)),
    ((20, 50), (308, 73))
])
def test_bbox_to_dimensions(resolution, expected_dimensions):
    bbox = BBox(((111.644, 8.655), (111.7, 8.688)), CRS.WGS84)
    dimensions = geo_utils.bbox_to_dimensions(bbox, resolution)

    assert dimensions == expected_dimensions


def test_get_image_dimensions():
    bbox = BBox(((111.644, 8.655), (111.7, 8.688)), CRS.WGS84)
    width = geo_utils.get_image_dimension(bbox, height=715)
    height = geo_utils.get_image_dimension(bbox, width=1202)

    assert width == 1203
    assert height == 715


def test_bbox_transform():
    bbox = BBox(((111.644, 8.655), (111.7, 8.688)), CRS.WGS84)
    new_bbox = bbox.transform(CRS.POP_WEB)
    expected_bbox = BBox((12428153.23, 967155.41, 12434387.12, 970871.43), CRS.POP_WEB)

    assert tuple(new_bbox) == pytest.approx(tuple(expected_bbox), rel=1e-8)
    assert new_bbox.crs is expected_bbox.crs


@pytest.mark.parametrize('point, source_crs, target_crs, target_point', [
    ((111.644, 8.655), CRS.WGS84, CRS.POP_WEB, (12428153.23, 967155.41)),
    ((360000.0, 4635040.0), CRS.UTM_31N, CRS.WGS84, (1.313392213, 41.854888581)),
    ((360000.0, 4635040.0), CRS.UTM_31N, CRS.UTM_30N, (858072.82713, 4642667.30545)),
    ((1475000.0, 5100000.0), CRS(2193), CRS.WGS84, (171.43450808, -44.24250942)),
    ((543569.807, 6062625.7678), CRS(3346), CRS.UTM_35N, (350231.496834, 6063682.846723))
])
def test_transform_point(point, source_crs, target_crs, target_point):
    new_point = geo_utils.transform_point(point, source_crs, target_crs)
    new_source_point = geo_utils.transform_point(new_point, target_crs, source_crs)

    assert new_point == pytest.approx(target_point, rel=1e-8)
    assert new_source_point == pytest.approx(point, rel=1e-8)

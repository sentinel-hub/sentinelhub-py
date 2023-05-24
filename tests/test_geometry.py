import copy
import warnings
from typing import Any, Tuple, TypeVar

import pytest
import shapely.geometry
from pytest import approx

from sentinelhub import CRS, BBox, Geometry, get_utm_crs
from sentinelhub.exceptions import SHDeprecationWarning

GeoType = TypeVar("GeoType", BBox, Geometry)

WKT_STRING = (
    "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), "
    "(30 20, 20 15, 20 25, 30 20)))"
)
GEOMETRY1 = Geometry(shapely.geometry.Polygon([(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]), CRS(32633))
GEOMETRY2 = Geometry(WKT_STRING, CRS.WGS84)
BBOX = BBox(bbox=(14.00, 45.00, 14.03, 45.03), crs=CRS.WGS84)

GEOMETRY_LIST = [GEOMETRY1, GEOMETRY2, BBOX]


@pytest.mark.parametrize(
    ("coords", "crs"),
    [
        ([[46.07, 13.23], [46.24, 13.57]], CRS.WGS84),
        ((46.07, 13.23, 46.24, 13.57), CRS.POP_WEB),
        (((46.07, 13.23), (46.24, 13.57)), CRS(8687)),
        ([(46.07, 13.23), (46.24, 13.57)], CRS.WGS84),
        ({"min_x": 46.07, "min_y": 13.23, "max_x": 46.24, "max_y": 13.57}, CRS.POP_WEB),
    ],
)
def test_bbox_different_input_options(coords: Any, crs: CRS) -> None:
    bbox = BBox(coords, crs)
    assert bbox.upper_right == (46.24, 13.57)
    assert bbox.lower_left == (46.07, 13.23)
    assert bbox.crs == crs


@pytest.mark.parametrize(
    ("coords", "crs"),
    [
        ({"x1": 46.07, "y1": 13.23, "x2": 46.24, "y2": 13.57}, CRS.WGS84),
        ((46.07, 13.23, 46.24, 13.57), None),
        ((46.07, 13.23, (46.24, 13.57)), CRS.WGS84),
    ],
)
def test_bbox_bad_input_options(coords: Any, crs: CRS) -> None:
    with pytest.raises((KeyError, ValueError)):
        BBox(coords, crs)


def test_bbox_to_str() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", SHDeprecationWarning)
        bbox = BBox(((45.0, 12.0, 47.0, 14.0)), CRS.WGS84)
        assert str(bbox) == "45.0,12.0,47.0,14.0"


@pytest.mark.parametrize(
    ("coords", "crs", "expected"),
    [
        ((46.07, 13.23, 46.24, 13.57), CRS(4326), "BBox(((46.07, 13.23), (46.24, 13.57)), crs=CRS('4326'))"),
        (((42, 13.23), (47.453, 18.57)), CRS.POP_WEB, "BBox(((42.0, 13.23), (47.453, 18.57)), crs=CRS('3857'))"),
    ],
)
def test_bbox_repr(coords: Any, crs: CRS, expected: str) -> None:
    assert repr(BBox(coords, crs)) == expected


def test_bbox_iter() -> None:
    assert tuple(BBOX) == (14.00, 45.00, 14.03, 45.03)
    assert list(BBOX) == [14.00, 45.00, 14.03, 45.03]


@pytest.mark.parametrize(
    ("bbox1", "bbox2"),
    [
        [BBOX, BBOX],
        [BBox([46.07, 13.23, 46.24, 13.57], CRS.WGS84), BBox(((46.07, 13.23), (46.24, 13.57)), crs=CRS(4326))],
        [BBox(((0, 0), (1, 1)), CRS(1234)), BBox({"min_x": 0, "min_y": 0, "max_x": 1, "max_y": 1}, CRS("epsg:1234"))],
    ],
)
def test_bbox_eq_true(bbox1: BBox, bbox2: BBox) -> None:
    assert bbox1 == bbox2


@pytest.mark.parametrize(
    ("bbox1", "bbox2"),
    [
        pytest.param(BBox((0, 0, 1, 1), CRS(1234)), (0, 0, 1, 1), id="different_types"),
        pytest.param(BBox((0, 0, 1, 1), CRS(1234)), BBox((0, 0, 1, 1), CRS(4321)), id="different_CRS"),
        pytest.param(BBox((0, 0, 1, 1), CRS(1234)), BBox((0, 0.00000001, 1, 1), CRS(1234)), id="different_coords"),
    ],
)
def test_bbox_eq_false(bbox1: BBox, bbox2: BBox) -> None:
    assert bbox1 != bbox2


def test_bbox_transform() -> None:
    original_bbox = BBox((46.07, 13.23, 46.24, 13.57), CRS.WGS84)
    transformed_bbox = original_bbox.transform(CRS.POP_WEB)

    assert transformed_bbox.crs == CRS.POP_WEB
    assert list(transformed_bbox) == approx([5128488.941, 1486021.486, 5147413.254, 1524929.4087], rel=1e-10)

    reconstructed_bbox = transformed_bbox.transform(CRS.WGS84)

    assert list(original_bbox) == approx(list(reconstructed_bbox), rel=1e-10)
    assert original_bbox.crs == reconstructed_bbox.crs


def test_bbox_transform_bounds() -> None:
    original_bbox = BBox((46.07, 13.23, 46.24, 13.57), CRS.WGS84)
    utm_crs = get_utm_crs(*original_bbox.middle, source_crs=CRS.WGS84)
    reconstructed_bbox = original_bbox.transform_bounds(utm_crs).transform_bounds(CRS.WGS84)

    assert reconstructed_bbox.geometry.contains(original_bbox.geometry)

    area_diff = reconstructed_bbox.geometry.difference(original_bbox.geometry).area
    expected_diff = reconstructed_bbox.geometry.area / 20  # the area difference for this case is about 2.5%
    assert area_diff < expected_diff


def test_bbox_geometry_attribute() -> None:
    bbox = BBox((0, 0, 1, 1), CRS.WGS84)
    assert isinstance(bbox.geometry, shapely.geometry.Polygon)
    assert bbox.geometry.equals(shapely.geometry.Polygon([[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]))


@pytest.mark.parametrize(
    ("bbox", "rel_buffered", "abs_buffered"),
    [
        [BBox((10, 10, 20, 20), CRS.WGS84), (5, 5, 25, 25), (9.8, 9.8, 20.2, 20.2)],
        [BBox((46.05, 13.21, 47.40, 13.41), CRS.POP_WEB), (45.375, 13.11, 48.075, 13.51), (45.85, 13.01, 47.6, 13.61)],
    ],
)
def test_bbox_buffer(bbox, rel_buffered, abs_buffered) -> None:
    for relative in (True, False):
        assert bbox.buffer(3.7, relative=relative).crs == bbox.crs

    assert bbox.buffer(0) is not bbox
    assert bbox.buffer(0) == bbox

    assert tuple(bbox.buffer(1)) == approx(rel_buffered)
    assert tuple(bbox.buffer(0.2, relative=False)) == approx(abs_buffered)

    assert bbox == bbox.buffer((10, -0.1)).buffer((-10 / 11, 1 / 9))
    assert bbox == bbox.buffer((-0.01, 0.2), relative=False).buffer((0.01, -0.2), relative=False)


@pytest.mark.parametrize(("buffer", "relative"), [(-1, True), ((1, -0.5), False)])
def test_bbox_buffer_fault_input(buffer, relative) -> None:
    bbox = BBox((46.05, 13.21, 47.40, 13.41), CRS.POP_WEB)
    with pytest.raises(ValueError):
        bbox.buffer(buffer, relative=relative)


@pytest.mark.parametrize("geometry", GEOMETRY_LIST)
def test_geometry_repr(geometry: GeoType) -> None:
    assert isinstance(repr(geometry), str)


@pytest.mark.parametrize("geometry", GEOMETRY_LIST)
def test_geometry_eq(geometry: GeoType) -> None:
    assert geometry == copy.deepcopy(geometry), "Deep copied object should be equal to the original"
    assert geometry != geometry.geometry


@pytest.mark.parametrize("geometry", GEOMETRY_LIST)
def test_geometry_reverse(geometry: GeoType) -> None:
    reversed_geometry = geometry.reverse()
    assert geometry != reversed_geometry
    assert geometry == reversed_geometry.reverse(), "Twice reversed geometry should equal the original"


@pytest.mark.parametrize("geometry", GEOMETRY_LIST)
@pytest.mark.parametrize("new_crs", [CRS.POP_WEB, CRS(32737)])
def test_transform_geometry(new_crs: CRS, geometry: GeoType) -> None:
    new_geometry = geometry.transform(new_crs)
    assert new_geometry.crs == new_crs
    assert geometry != new_geometry, "Transformed geometry should be different"

    reconstructed_geometry = new_geometry.transform(geometry.crs)
    assert geometry.crs == reconstructed_geometry.crs
    assert geometry.geometry.equals_exact(reconstructed_geometry.geometry, tolerance=1e-6)


def test_geometry_geojson_parameter_with_crs() -> None:
    expected_without_crs = {
        "type": "Polygon",
        "coordinates": (((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)),),
    }
    expected_with_crs = {
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::32633"}},
        **expected_without_crs,
    }
    assert GEOMETRY1.geojson == GEOMETRY1.get_geojson()
    assert GEOMETRY1.get_geojson(with_crs=False) == expected_without_crs
    assert GEOMETRY1.get_geojson(with_crs=True) == expected_with_crs


@pytest.mark.parametrize("geometry", [GEOMETRY1, GEOMETRY2])
def test_geometry_geojson_reconstructible(geometry: Geometry) -> None:
    assert geometry == Geometry(geometry.geojson, geometry.crs)
    assert geometry == Geometry.from_geojson(geometry.geojson)


def test_geometry_wkt() -> None:
    for geometry in [GEOMETRY1, GEOMETRY2]:
        assert geometry == Geometry(geometry.wkt, geometry.crs)

    assert GEOMETRY2.wkt == WKT_STRING, "New WKT string does not match the original"


@pytest.mark.parametrize("geometry", [GEOMETRY1, GEOMETRY2])
def test_bbox_of_geometry(geometry: Geometry) -> None:
    assert geometry.bbox == BBox(geometry.geometry.bounds, geometry.crs)


@pytest.mark.parametrize(
    ("input_geometry", "expected_output_geometry"),
    [
        (BBox((1.11, 0, 0.999, 0.05), crs=CRS.WGS84), BBox((1.1, 0, 1.0, 0.1), crs=CRS.WGS84)),
        (
            Geometry("POLYGON ((0 0, 1.001 0.99, -0.1 0.45, 0 0))", crs=CRS.WGS84),
            Geometry("POLYGON ((0 0, 1.0 1.0, -0.1 0.5, 0 0))", crs=CRS.WGS84),
        ),
    ],
)
def test_geometry_apply_method(input_geometry: GeoType, expected_output_geometry: GeoType) -> None:
    def _round_point_coords(x: float, y: float, decimals: int = 1) -> Tuple[float, float]:
        return round(x, decimals), round(y, decimals)

    rounded_geometry = input_geometry.apply(_round_point_coords)

    assert rounded_geometry is not input_geometry
    assert rounded_geometry == expected_output_geometry

import copy

import shapely.geometry
import pytest
from pytest import approx

from sentinelhub import BBox, Geometry, BBoxCollection, CRS, get_utm_crs

WKT_STRING = (
    'MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), '
    '(30 20, 20 15, 20 25, 30 20)))'
)
polygon = shapely.geometry.Polygon([(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)])
GEOMETRY1 = Geometry(polygon, CRS(32633))
GEOMETRY2 = Geometry(WKT_STRING, CRS.WGS84)
BBOX = BBox(bbox=[14.00, 45.00, 14.03, 45.03], crs=CRS.WGS84)
BBOX_COLLECTION = BBoxCollection([BBOX, BBox('46,13,47,20', CRS.WGS84)])

GEOMETRY_LIST = [GEOMETRY1, GEOMETRY2, BBOX_COLLECTION, BBOX]


def test_bbox_no_crs():
    with pytest.raises(TypeError):
        BBox('46,13,47,20')


def test_bbox_from_string():
    bbox_str = '46.07, 13.23, 46.24, 13.57'
    bbox = BBox(bbox_str, CRS.WGS84)
    assert bbox.lower_left == (46.07, 13.23)
    assert bbox.upper_right == (46.24, 13.57)
    assert bbox.crs == CRS.WGS84


def test_bbox_from_bad_string():
    with pytest.raises(ValueError):
        # Too few coordinates
        BBox('46.07, 13.23, 46.24', CRS.WGS84)

    with pytest.raises(ValueError):
        # Invalid string
        BBox('46N,13E,45N,12E', CRS.WGS84)


@pytest.mark.parametrize('bbox_lst', [
    [46.07, 13.23, 46.24, 13.57],
    [46.24, 13.23, 46.07, 13.57],
    [46.07, 13.57, 46.24, 13.23],
    [46.24, 13.57, 46.07, 13.23],
])
def test_bbox_from_flat_list(bbox_lst):
    bbox = BBox(bbox_lst, CRS.WGS84)
    assert bbox.lower_left == (46.07, 13.23)
    assert bbox.upper_right == (46.24, 13.57)
    assert bbox.crs == CRS.WGS84


@pytest.mark.parametrize('bbox_input', [
    [[46.07, 13.23], [46.24, 13.57]],
    (46.07, 13.23, 46.24, 13.57),
    ((46.07, 13.23), (46.24, 13.57)),
    [(46.07, 13.23), (46.24, 13.57)],
    {'min_x': 46.07, 'min_y': 13.23, 'max_x': 46.24, 'max_y': 13.57},
    BBox({'min_x': 46.07, 'min_y': 13.23, 'max_x': 46.24, 'max_y': 13.57}, CRS.WGS84),
])
def test_bbox_different_input(bbox_input):
    bbox = BBox(bbox_input, CRS.WGS84)
    assert bbox.upper_right == (46.24, 13.57)
    assert bbox.lower_left == (46.07, 13.23)
    assert bbox.crs == CRS.WGS84


def test_bbox_from_bad_dict():
    bbox_dict = {'x1': 46.07, 'y1': 13.23, 'x2': 46.24, 'y2': 13.57}
    with pytest.raises(KeyError):
        BBox(bbox_dict, CRS.WGS84)


@pytest.mark.parametrize('bbox_input', [
    shapely.geometry.LineString([(0, 0), (1, 1)]),
    shapely.geometry.LinearRing([(1, 0), (1, 1), (0, 0)]),
    shapely.geometry.Polygon([(1, 0), (1, 1), (0, 0)])
])
def test_bbox_from_shapely(bbox_input):
    assert BBox(bbox_input, CRS.WGS84) == BBox((0, 0, 1, 1), CRS.WGS84)


def test_bbox_to_str():
    x1, y1, x2, y2 = 45.0, 12.0, 47.0, 14.0
    crs = CRS.WGS84
    expect_str = f'{x1},{y1},{x2},{y2}'
    bbox = BBox(((x1, y1), (x2, y2)), crs)
    assert str(bbox) == expect_str


def test_bbox_to_repr():
    x1, y1, x2, y2 = 45.0, 12.0, 47.0, 14.0
    bbox = BBox(((x1, y1), (x2, y2)), crs=CRS('4326'))
    expect_repr = f"BBox((({x1}, {y1}), ({x2}, {y2})), crs=CRS('4326'))"
    assert repr(bbox) == expect_repr


def test_bbox_iter():
    bbox_lst = [46.07, 13.23, 46.24, 13.57]
    bbox = BBox(bbox_lst, CRS.WGS84)
    list_from_bbox_iter = list(bbox)
    assert list_from_bbox_iter == bbox_lst


def test_bbox_eq():
    bbox1 = BBox([46.07, 13.23, 46.24, 13.57], CRS.WGS84)
    bbox2 = BBox(((46.24, 13.57), (46.07, 13.23)), 4326)
    bbox3 = BBox([46.07, 13.23, 46.24, 13.57], CRS.POP_WEB)
    bbox4 = BBox([46.07, 13.23, 46.24, 13.58], CRS.WGS84)
    assert bbox1 == bbox2
    assert bbox1 != bbox3
    assert bbox1 != bbox4
    assert bbox1 is not None


def test_transform():
    bbox1 = BBox([46.07, 13.23, 46.24, 13.57], CRS.WGS84)
    bbox2 = bbox1.transform(CRS.POP_WEB).transform(CRS.WGS84)

    for coord1, coord2 in zip(bbox1, bbox2):
        assert coord1 == approx(coord2, abs=1e-8)
    assert bbox1.crs == bbox2.crs


def test_transform_bounds():
    bbox1 = BBox([46.07, 13.23, 46.24, 13.57], CRS.WGS84)
    utm_crs = get_utm_crs(*bbox1.middle, CRS.WGS84)
    bbox2 = bbox1.transform_bounds(utm_crs).transform_bounds(CRS.WGS84)

    assert bbox2.geometry.contains(bbox1.geometry)
    assert bbox2.geometry.difference(bbox1.geometry).area > 1e-4


def test_geometry():
    bbox = BBox([46.07, 13.23, 46.24, 13.57], CRS.WGS84)
    assert isinstance(bbox.get_geojson(), dict)
    assert isinstance(bbox.geometry, shapely.geometry.Polygon)


def test_buffer():
    bbox = BBox([46.07, 13.23, 46.24, 13.57], CRS.WGS84)

    assert bbox == bbox.buffer(0), 'Buffer 0 should not change bounding box'
    assert bbox == bbox.buffer(1).buffer(-0.5), 'Twice buffered bounding box should return to original'


@pytest.mark.parametrize('geometry', GEOMETRY_LIST)
def test_repr(geometry):
    assert isinstance(repr(geometry), str)


@pytest.mark.parametrize('geometry', GEOMETRY_LIST)
def test_eq(geometry):
    assert geometry == copy.deepcopy(geometry), 'Deep copied object should be equal to the original'
    assert geometry is not None


@pytest.mark.parametrize('geometry', GEOMETRY_LIST)
def test_reverse(geometry):
    reversed_geometry = geometry.reverse()
    assert geometry != reversed_geometry
    assert geometry == reversed_geometry.reverse(), 'Twice reversed geometry should equal the original'


@pytest.mark.parametrize('geometry', GEOMETRY_LIST)
def test_transform_geometry(geometry):
    new_geometry = geometry.transform(CRS.POP_WEB)
    assert geometry != new_geometry, 'Transformed geometry should be different'

    original_geometry = geometry.transform(geometry.crs)
    assert geometry.crs == original_geometry.crs, 'CRS of twice transformed geometry should preserve'
    assert geometry.geometry.area == approx(original_geometry.geometry.area, abs=1e-10), 'Geometry area should be equal'


@pytest.mark.parametrize('geometry', [GEOMETRY1, GEOMETRY2])
def test_geojson(geometry):
    assert geometry == Geometry(geometry.geojson, geometry.crs), \
        'Transforming geometry to geojson and back should preserve it'
    assert geometry == Geometry.from_geojson(geometry.geojson)
    assert geometry == Geometry.from_geojson(geometry.get_geojson())


def test_geojson_parameter_with_crs():
    expected_without_crs = {
        'type': 'Polygon',
        'coordinates': (((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)),)
    }
    expected_with_crs = {
        'crs': {'type': 'name', 'properties': {'name': 'urn:ogc:def:crs:EPSG::32633'}},
        **expected_without_crs
    }
    assert GEOMETRY1.get_geojson(with_crs=False) == expected_without_crs
    assert GEOMETRY1.get_geojson(with_crs=True) == expected_with_crs


def test_wkt():
    for geometry in [GEOMETRY1, GEOMETRY2]:
        assert geometry == Geometry(geometry.wkt, geometry.crs), \
            'Transforming geometry to wkt and back should preserve it'

    assert GEOMETRY2.wkt == WKT_STRING, 'New WKT string does not match the original'


@pytest.mark.parametrize('geometry', [GEOMETRY1, GEOMETRY2, BBOX_COLLECTION])
def test_bbox(geometry):
    assert geometry.bbox == BBox(geometry.geometry, geometry.crs), 'Failed bbox property'

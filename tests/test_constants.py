import warnings

import pyproj
import pytest

from sentinelhub import CRS, MimeType
from sentinelhub.constants import RequestType
from sentinelhub.exceptions import SHUserWarning


@pytest.mark.parametrize('lng, lat, epsg', [
    (13, 46, '32633'),
    (13, 0, '32633'),
    (13, -45, '32733'),
    (13, 0, '32633'),
    (13, -0.0001, '32733'),
    (13, -46, '32733'),
])
def test_utm(lng, lat, epsg):
    crs = CRS.get_utm_from_wgs84(lng, lat)
    assert epsg == crs.value


@pytest.mark.parametrize('parse_value, expected', [
    (4326, CRS.WGS84),
    ('4326', CRS.WGS84),
    ('EPSG:3857', CRS.POP_WEB),
    ({'init': 'EPSG:32638'}, CRS.UTM_38N),
    (pyproj.CRS('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'), CRS.WGS84),
    ('urn:ogc:def:crs:epsg::32631', CRS.UTM_31N),
    ('urn:ogc:def:crs:OGC::CRS84', CRS.WGS84),
    (pyproj.CRS(3857), CRS.POP_WEB),
])
def test_crs_parsing(parse_value, expected):
    parsed_result = CRS(parse_value)
    assert parsed_result == expected


@pytest.mark.parametrize('parse_value, expected, warning', [
    (pyproj.CRS(4326), CRS.WGS84, SHUserWarning)
])
def test_crs_parsing_warn(parse_value, expected, warning):
    with pytest.warns(warning):
        parsed_result = CRS(parse_value)
        assert parsed_result == expected


def test_failed_crs_parsing():
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        wgs84_with_init = pyproj.CRS({'init': 'epsg:4326'})

    with pytest.raises(ValueError):
        CRS(wgs84_with_init)


@pytest.mark.parametrize('crs, epsg', [
    (CRS.POP_WEB, 'EPSG:3857'),
    (CRS.WGS84, 'EPSG:4326'),
    (CRS.UTM_33N, 'EPSG:32633'),
    (CRS.UTM_33S, 'EPSG:32733'),
])
def test_ogc_string(crs, epsg):
    ogc_str = CRS.ogc_string(crs)
    assert epsg == ogc_str


@pytest.mark.parametrize('crs, crs_repr', [
    (CRS.POP_WEB, "CRS('3857')"),
    (CRS.WGS84, "CRS('4326')"),
    (CRS.UTM_33N, "CRS('32633')"),
    (CRS.UTM_33S, "CRS('32733')"),
    (CRS('3857'), "CRS('3857')"),
    (CRS('4326'), "CRS('4326')"),
    (CRS('32633'), "CRS('32633')"),
    (CRS('32733'), "CRS('32733')"),
])
def test_crs_repr(crs, crs_repr):
    assert crs_repr == repr(crs)


@pytest.mark.parametrize('crs', CRS)
def test_crs_has_value(crs):
    assert CRS.has_value(crs.value), f'Expected support for CRS {crs.value}'


@pytest.mark.parametrize('value, fails', [
    ('string', True), (-1, True), (999, True), (None, True), (3035, False), ('EPSG:3035', False), (10000, False)

])
def test_custom_crs(value, fails):
    if fails:
        with pytest.raises(ValueError):
            CRS(value)
    else:
        CRS(CRS(value))

        new_enum_value = str(value).lower().strip('epsg: ')
        assert CRS.has_value(new_enum_value)


@pytest.mark.parametrize('crs', [CRS.WGS84, CRS.POP_WEB, CRS.UTM_38N])
def test_pyproj_methods(crs):
    assert isinstance(crs.projection(), pyproj.Proj)
    assert isinstance(crs.pyproj_crs(), pyproj.CRS)


@pytest.mark.parametrize('ext, mime_type', [
    ('tif', MimeType.TIFF),
    ('jpeg', MimeType.JPG),
    ('jpg', MimeType.JPG),
    ('h5', MimeType.HDF),
    ('hdf5', MimeType.HDF),
    *[(mime_type.extension, mime_type) for mime_type in MimeType]
])
def test_mime_type_from_string(ext, mime_type):
    assert MimeType.from_string(ext) == mime_type


@pytest.mark.parametrize('faulty_arg', ['unknown ext', 'tiff;depth=32f'])
def test_mimetype_no_value_fail(faulty_arg):
    assert not MimeType.has_value(faulty_arg), 'This value is not supposed to be type of the Enum'
    with pytest.raises(ValueError):
        MimeType.from_string(faulty_arg)


@pytest.mark.parametrize('ext', ['tif', 'tiff', 'jpg', 'jpeg', 'png', 'jp2'])
def test_is_image_format(ext):
    mime_type = MimeType.from_string(ext)
    assert MimeType.is_image_format(mime_type)


@pytest.mark.parametrize('mime_type, expected_string', [
    (MimeType.PNG, 'image/png'),
    (MimeType.JPG, 'image/jpeg'),
    (MimeType.TIFF, 'image/tiff'),
    (MimeType.JSON, 'application/json'),
    (MimeType.CSV, 'text/csv'),
    (MimeType.ZIP, 'application/zip'),
    (MimeType.HDF, 'application/x-hdf'),
    (MimeType.XML, 'text/xml'),
    (MimeType.TXT, 'text/plain'),
    (MimeType.TAR, 'application/x-tar'),
])
def test_get_string(mime_type, expected_string):
    assert MimeType.get_string(mime_type) == expected_string
    assert MimeType.from_string(expected_string) == mime_type, 'Result of `get_string` not accepted by `from_string`'


def test_get_expected_max_value():
    assert MimeType.TIFF.get_expected_max_value() == 65535
    assert MimeType.PNG.get_expected_max_value() == 255

    with pytest.raises(ValueError):
        MimeType.TAR.get_expected_max_value()


def test_request_type():
    with pytest.raises(ValueError):
        RequestType('post')

    with pytest.raises(ValueError):
        RequestType('get')

    # check that this goes through without errors
    RequestType('POST')
    RequestType('GET')

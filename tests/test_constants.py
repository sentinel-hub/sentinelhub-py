"""
Tests for constants.py module
"""
from typing import Any, Type

import numpy as np
import pyproj
import pytest

from sentinelhub import CRS, MimeType
from sentinelhub.constants import RequestType, ResamplingType
from sentinelhub.exceptions import SHUserWarning


@pytest.mark.parametrize(
    "lng, lat, expected_crs",
    [
        (13, 46, CRS("32633")),
        (13, 0, CRS("32633")),
        (13, -45, CRS("32733")),
        (13, 0, CRS("32633")),
        (13, -0.0001, CRS("32733")),
        (13, -46, CRS("32733")),
    ],
)
def test_utm_from_wgs84(lng: float, lat: float, expected_crs: CRS) -> None:
    assert CRS.get_utm_from_wgs84(lng, lat) is expected_crs


@pytest.mark.parametrize(
    "crs_input, expected",
    [
        (4326, CRS.WGS84),
        (np.int64(4326), CRS.WGS84),
        (np.uint16(4326), CRS.WGS84),
        ("4326", CRS.WGS84),
        ("EPSG:3857", CRS.POP_WEB),
        ({"init": "EPSG:32638"}, CRS.UTM_38N),
        (pyproj.CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"), CRS.WGS84),
        (pyproj.CRS(CRS.WGS84.pyproj_crs().to_wkt()), CRS.WGS84),
        ("urn:ogc:def:crs:epsg::32631", CRS.UTM_31N),
        ("urn:ogc:def:crs:OGC::CRS84", CRS.WGS84),
        (pyproj.CRS(3857), CRS.POP_WEB),
    ],
)
def test_crs_input(crs_input: object, expected: CRS) -> None:
    assert CRS(crs_input) is expected


@pytest.mark.parametrize("crs_input, expected, warning", [(pyproj.CRS(4326), CRS.WGS84, SHUserWarning)])
def test_crs_input_warn(crs_input: Any, expected: CRS, warning: Type[Warning]) -> None:
    with pytest.warns(warning):
        parsed_result = CRS(crs_input)
        assert parsed_result == expected


@pytest.mark.parametrize("bad_input", ["string", "12", -1, 999, None, 3035.5])
def test_crs_faulty_input(bad_input: object) -> None:
    with pytest.raises(ValueError):
        CRS(bad_input)


@pytest.mark.parametrize(
    "crs, epsg",
    [(CRS.POP_WEB, "EPSG:3857"), (CRS.WGS84, "EPSG:4326"), (CRS.UTM_33N, "EPSG:32633"), (CRS.UTM_33S, "EPSG:32733")],
)
def test_ogc_string(crs: CRS, epsg: str) -> None:
    assert crs.ogc_string() == epsg


@pytest.mark.parametrize(
    "crs, expected_repr",
    [
        (CRS.POP_WEB, "CRS('3857')"),
        (CRS.WGS84, "CRS('4326')"),
        (CRS.UTM_33N, "CRS('32633')"),
        (CRS.UTM_33S, "CRS('32733')"),
        (CRS("3857"), "CRS('3857')"),
        (CRS("4326"), "CRS('4326')"),
        (CRS("32633"), "CRS('32633')"),
        (CRS("32733"), "CRS('32733')"),
    ],
)
def test_crs_repr(crs: CRS, expected_repr: str) -> None:
    assert repr(crs) == expected_repr


@pytest.mark.parametrize("crs", CRS)
def test_crs_has_value(crs: CRS) -> None:
    assert CRS.has_value(crs.value), f"Expected support for CRS {crs.value}"


@pytest.mark.parametrize("crs_input, crs_value", [(3035, "3035"), ("EPSG:3035", "3035"), (10000, "10000")])
def test_crs_not_predefined(crs_input: object, crs_value: str) -> None:
    crs = CRS(crs_input)
    assert crs.value == crs_value
    assert CRS.has_value(crs_value)


@pytest.mark.parametrize("crs", [CRS.WGS84, CRS.POP_WEB, CRS.UTM_38N])
def test_pyproj_methods(crs: CRS) -> None:
    assert isinstance(crs.projection(), pyproj.Proj)
    assert isinstance(crs.pyproj_crs(), pyproj.CRS)


@pytest.mark.parametrize(
    "ext, mime_type",
    [
        ("tif", MimeType.TIFF),
        ("jpeg", MimeType.JPG),
        ("jpg", MimeType.JPG),
        ("h5", MimeType.HDF),
        ("hdf5", MimeType.HDF),
        *[(mime_type.extension, mime_type) for mime_type in MimeType],
    ],
)
def test_mime_type_from_string(ext: str, mime_type: MimeType) -> None:
    assert MimeType.from_string(ext) == mime_type


@pytest.mark.parametrize("faulty_arg", ["unknown ext", "tiff;depth=32f"])
def test_mimetype_no_value_fail(faulty_arg: str) -> None:
    assert not MimeType.has_value(faulty_arg), "This value is not supposed to be type of the Enum"
    with pytest.raises(ValueError):
        MimeType.from_string(faulty_arg)


@pytest.mark.parametrize("mime_type", MimeType)
def test_is_image_format(mime_type: MimeType) -> None:
    expected_to_be_image = mime_type in {MimeType.TIFF, MimeType.PNG, MimeType.JP2, MimeType.JPG}
    assert MimeType.is_image_format(mime_type) == expected_to_be_image


@pytest.mark.parametrize("mime_type", MimeType)
def test_is_api_format(mime_type: MimeType) -> None:
    expected_to_be_api_format = mime_type in {MimeType.JPG, MimeType.PNG, MimeType.TIFF, MimeType.JSON}
    assert MimeType.is_api_format(mime_type) == expected_to_be_api_format


@pytest.mark.parametrize(
    "mime_type, expected_string",
    [
        (MimeType.PNG, "image/png"),
        (MimeType.JPG, "image/jpeg"),
        (MimeType.TIFF, "image/tiff"),
        (MimeType.JSON, "application/json"),
        (MimeType.CSV, "text/csv"),
        (MimeType.ZIP, "application/zip"),
        (MimeType.HDF, "application/x-hdf"),
        (MimeType.XML, "text/xml"),
        (MimeType.TXT, "text/plain"),
        (MimeType.TAR, "application/x-tar"),
    ],
)
def test_get_string(mime_type: MimeType, expected_string: str) -> None:
    assert MimeType.get_string(mime_type) == expected_string
    assert MimeType.from_string(expected_string) == mime_type, "Result of `get_string` not accepted by `from_string`"


@pytest.mark.parametrize(
    "mime_type, path, expected_answer",
    [
        (MimeType.NPY, "some/path/file.npy", True),
        (MimeType.PNG, "./file.PNG", True),
        (MimeType.GPKG, "file.gpkg.gz", False),
        (MimeType.JSON, "path/to/file.geojson", False),
    ],
)
def test_matches_extension(mime_type: MimeType, path: str, expected_answer: bool) -> None:
    assert mime_type.matches_extension(path) == expected_answer


def test_get_expected_max_value() -> None:
    assert MimeType.TIFF.get_expected_max_value() == 65535
    assert MimeType.PNG.get_expected_max_value() == 255
    assert MimeType.JPG.get_expected_max_value() == 255
    assert MimeType.JP2.get_expected_max_value() == 10000

    with pytest.raises(ValueError):
        MimeType.TAR.get_expected_max_value()


def test_request_type() -> None:
    with pytest.raises(ValueError):
        RequestType("post")

    with pytest.raises(ValueError):
        RequestType("get")

    # check that this goes through without errors
    RequestType("POST")
    RequestType("GET")


def test_resampling_type_not_case_sensitive() -> None:
    ResamplingType("nearest")
    ResamplingType("Nearest")
    ResamplingType("NEAREST")
    with pytest.raises(ValueError):
        ResamplingType("nyearest")

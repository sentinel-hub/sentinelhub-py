import os
from typing import Tuple, Union
from xml.etree import ElementTree

import numpy as np
import pytest
from fs.tempfs import TempFS
from pytest import approx
from pytest_lazyfixture import lazy_fixture

from sentinelhub import read_data, write_data

BASIC_IMAGE = np.arange((5 * 6 * 3), dtype=np.uint8).reshape((5, 6, 3))


@pytest.mark.parametrize(
    ("filename", "mean", "shape"),
    [
        ("img.tif", 13577.494856, (2048, 2048, 3)),
        ("img.png", 52.33736, (2048, 2048, 3)),
        ("img.jpg", 52.412425, (2048, 2048, 3)),
        ("img-8bit.jp2", 47.09060, (343, 343, 3)),
        ("img-15bit.jp2", 0.3041897, (1830, 1830)),
        ("img-16bit.jp2", 0.3041897, (1830, 1830)),
    ],
)
def test_img_read(input_folder: str, filename: str, mean: float, shape: Tuple[int, ...]) -> None:
    img = read_data(os.path.join(input_folder, filename))

    assert img.shape == shape
    assert np.mean(img) == approx(mean, abs=1e-4)
    assert img.flags["WRITEABLE"], "Obtained numpy array is not writeable"


def test_read_tar_with_folder(input_folder: str) -> None:
    path = os.path.join(input_folder, "tar-folder.tar")
    data = read_data(path)

    assert data == {"tar-folder/simple.json": {"message": "test"}}


@pytest.fixture()
def xml_testcase():
    xml_root = ElementTree.Element("EOPatch")
    xml_data = ElementTree.SubElement(xml_root, "data")
    ElementTree.SubElement(xml_data, "field1", name="BANDS-S2-L1C").text = "some value1"
    ElementTree.SubElement(xml_data, "field2", name="CLP").text = "some value2"
    return ElementTree.ElementTree(xml_root)


@pytest.mark.parametrize(
    ("filename", "data"),
    [
        ("img.tif", np.arange(5 * 5 * 3).reshape((5, 5, 3))),  # not restricting dtype
        ("img.png", BASIC_IMAGE),
        ("img-8bit.jp2", BASIC_IMAGE),
        ("img-15bit.jp2", BASIC_IMAGE),
        ("img-16bit.jp2", BASIC_IMAGE),
        ("test-string.txt", "sentinelhub-py is often shortened to sh-py"),
        ("test-xml.xml", lazy_fixture("xml_testcase")),
    ],
)
def test_write_read(filename: str, data: Union[str, np.ndarray, ElementTree.ElementTree]) -> None:
    with TempFS() as filesystem:
        file_path = filesystem.getsyspath(filename)
        write_data(file_path, data)
        assert filesystem.exists(filename)
        new_data = read_data(file_path)

        if isinstance(data, np.ndarray):
            assert np.array_equal(data, new_data), "Original and saved image are not the same"
        elif isinstance(data, ElementTree.ElementTree):
            assert set(data.getroot().itertext()) == set(new_data.getroot().itertext())
        else:
            assert data == new_data


@pytest.mark.parametrize("filename", ["img.jpg"])
def test_img_write_jpg(filename: str) -> None:
    # Cannot verify that data is written correctly because JPG is not a lossless format
    with TempFS() as filesystem:
        file_path = filesystem.getsyspath(filename)
        write_data(file_path, BASIC_IMAGE)

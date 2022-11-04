import os
from typing import Tuple
from xml.etree import ElementTree as ET

import numpy as np
import pytest
from fs.tempfs import TempFS
from pytest import approx

from sentinelhub import read_data, write_data
from sentinelhub.exceptions import SHUserWarning


@pytest.mark.parametrize(
    "filename, mean, shape",
    [
        ("img.tif", 13577.494856, (2048, 2048, 3)),
        ("img.png", 52.33736, (2048, 2048, 3)),
        ("img-8bit.jp2", 47.09060, (343, 343, 3)),
        ("img-15bit.jp2", 0.3041897, (1830, 1830)),
        ("img-16bit.jp2", 0.3041897, (1830, 1830)),
    ],
)
def test_img_read(input_folder: str, output_folder: str, filename: str, mean: float, shape: Tuple[int, ...]) -> None:
    img = read_data(os.path.join(input_folder, filename))

    assert img.shape == shape
    assert np.mean(img) == approx(mean, abs=1e-4)
    assert img.flags["WRITEABLE"], "Obtained numpy array is not writeable"

    output_file_path = os.path.join(output_folder, filename)
    write_data(output_file_path, img)
    new_img = read_data(output_file_path)

    assert np.array_equal(img, new_img), "Original and saved image are not the same"


@pytest.mark.parametrize("filename, mean, shape", [("img.jpg", 52.412425, (2048, 2048, 3))])
def test_img_read_jpeg(
    input_folder: str, output_folder: str, filename: str, mean: float, shape: Tuple[int, ...]
) -> None:
    img = read_data(os.path.join(input_folder, filename))

    assert img.shape == shape
    assert np.mean(img) == approx(mean, abs=1e-4)
    assert img.flags["WRITEABLE"], "Obtained numpy array is not writeable"

    output_file_path = os.path.join(output_folder, filename)

    with pytest.warns(SHUserWarning):
        write_data(output_file_path, img)


def test_read_write_text() -> None:
    with TempFS() as filesystem:
        file_path = filesystem.getsyspath("test-string.txt")

        text = "sentinelhub-py is often shortened to sh-py"
        write_data(file_path, text)
        assert text == read_data(file_path)


def test_read_write_xml() -> None:
    with TempFS() as filesystem:
        file_path = filesystem.getsyspath("test-xml.xml")

        root = ET.Element("EOPatch")
        data = ET.SubElement(root, "data")
        ET.SubElement(data, "field1", name="BANDS-S2-L1C").text = "some value1"
        ET.SubElement(data, "field2", name="CLP").text = "some value2"
        tree = ET.ElementTree(root)

        write_data(file_path, tree)
        tree_new = read_data(file_path)
        assert isinstance(tree_new, ET.ElementTree)
        assert set(tree_new.getroot().itertext()) == set(tree.getroot().itertext())


def test_read_tar_with_folder(input_folder: str) -> None:
    path = os.path.join(input_folder, "tar-folder.tar")
    data = read_data(path)

    assert data == {"tar-folder/simple.json": {"message": "test"}}

"""
Utility functions to read/write image data from/to file
"""

import csv
import json
import logging
import os
import warnings
from typing import Any, Callable, Dict, Optional, Union
from xml.etree import ElementTree

import numpy as np
import tifffile as tiff
from PIL import Image

from .constants import MimeType
from .decoding import decode_image_with_pillow, decode_jp2_image, decode_tar, get_data_format
from .exceptions import SHUserWarning
from .os_utils import create_parent_folder

LOGGER = logging.getLogger(__name__)

CSV_DELIMITER = ";"


def read_data(filename: str, data_format: Optional[MimeType] = None) -> Any:
    """Read image data from file

    This function reads input data from file. The format of the file
    can be specified in ``data_format``. If not specified, the format is
    guessed from the extension of the filename.

    :param filename: filename to read data from
    :param data_format: format of filename. Default is `None`
    :return: data read from filename
    :raises: exception if filename does not exist
    """
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Filename {filename} does not exist")

    if not isinstance(data_format, MimeType):
        data_format = get_data_format(filename)

    reader = _get_reader(data_format)

    try:
        return reader(filename)
    except BaseException as exception:
        # In case a procedure would read a lot of files and one would be corrupt this helps us figure out which one
        LOGGER.debug("Failed to read from file: %s", filename)
        raise exception


def _get_reader(data_format: MimeType) -> Callable[[str], Any]:
    """Provides a function for reading data in a given data format"""
    if data_format is MimeType.TIFF:
        return read_tiff_image
    if data_format is MimeType.JP2:
        return read_jp2_image
    if data_format.is_image_format():
        return read_image
    try:
        available_readers: Dict[MimeType, Callable[[str], Any]] = {
            MimeType.TAR: read_tar,
            MimeType.TXT: read_text,
            MimeType.RAW: _read_binary,
            MimeType.CSV: read_csv,
            MimeType.JSON: read_json,
            MimeType.XML: read_xml,
            MimeType.GML: read_xml,
            MimeType.SAFE: read_xml,
        }
        return available_readers[data_format]
    except KeyError as exception:
        raise ValueError(f"Reading data format {data_format} is not supported") from exception


def read_tar(filename: str) -> Dict[str, object]:
    """Read a tar from file"""
    with open(filename, "rb") as file:
        return decode_tar(file)  # type: ignore


def read_tiff_image(filename: str) -> Any:
    """Read data from TIFF file

    :param filename: name of TIFF file to be read
    :return: data stored in TIFF file
    """
    return tiff.imread(filename)


def read_jp2_image(filename: str) -> np.ndarray:
    """Read data from JPEG2000 file

    :param filename: name of JPEG2000 file to be read
    :return: data stored in JPEG2000 file
    """
    with open(filename, "rb") as file:
        return decode_jp2_image(file)


def read_image(filename: str) -> np.ndarray:
    """Read data from PNG or JPG file

    :param filename: name of PNG or JPG file to be read
    :return: data stored in JPG file
    """
    return decode_image_with_pillow(filename)


def read_text(filename: str) -> str:
    """Read data from text file

    :param filename: name of text file to be read
    :return: data stored in text file
    """
    with open(filename, "r") as file:
        return file.read()


def _read_binary(filename: str) -> bytes:
    """Reads data in bytes"""
    with open(filename, "rb") as file:
        return file.read()


def read_csv(filename: str, delimiter: str = CSV_DELIMITER) -> list:
    """Read data from CSV file

    :param filename: name of CSV file to be read
    :param delimiter: type of CSV delimiter. Default is ``;``
    :return: data stored in CSV file as list
    """
    with open(filename, "r") as file:
        return list(csv.reader(file, delimiter=delimiter))


def read_json(filename: str) -> Any:
    """Read data from JSON file

    :param filename: name of JSON file to be read
    :return: data stored in JSON file
    """
    with open(filename, "rb") as file:
        return json.load(file)


def read_xml(filename: str) -> ElementTree.ElementTree:
    """Read data from XML or GML file

    :param filename: name of XML or GML file to be read
    :return: data stored in XML file
    """
    return ElementTree.parse(filename)


def read_numpy(filename: str) -> np.ndarray:
    """Read data from numpy file

    :param filename: name of numpy file to be read
    :return: data stored in file as numpy array
    """
    return np.load(filename)


def write_data(
    filename: str, data: np.ndarray, data_format: Optional[MimeType] = None, compress: bool = False, add: bool = False
) -> None:
    """Write image data to file

    Function to write image data to specified file. If file format is not provided
    explicitly, it is guessed from the filename extension. If format is TIFF, geo
    information and compression can be optionally added.

    :param filename: name of file to write data to
    :param data: image data to write to file
    :param data_format: format of output file. Default is `None`
    :param compress: whether to compress data or not. Default is `False`
    :param add: whether to append to existing text file or not. Default is `False`
    :raises: exception if numpy format is not supported or file cannot be written
    """
    create_parent_folder(filename)

    if not isinstance(data_format, MimeType):
        data_format = get_data_format(filename)

    if data_format is MimeType.TIFF:
        return write_tiff_image(filename, data, compress)
    if data_format.is_image_format():
        return write_image(filename, data)
    if data_format is MimeType.TXT:
        return write_text(filename, data, add=add)

    try:
        available_writers: Dict[MimeType, Callable[[str, Any], None]] = {
            MimeType.RAW: write_bytes,
            MimeType.CSV: write_csv,
            MimeType.JSON: write_json,
            MimeType.XML: write_xml,
            MimeType.GML: write_xml,
        }
        return available_writers[data_format](filename, data)
    except KeyError as exception:
        raise ValueError(f"Writing data format {data_format} is not supported") from exception


def write_tiff_image(filename: str, image: np.ndarray, compress: bool = False) -> None:
    """Write image data to TIFF file

    :param filename: name of file to write data to
    :param image: image data to write to file
    :param compress: whether to compress data. If `True`, lzma compression is used. Default is `False`
    """
    if compress:
        return tiff.imwrite(filename, image, compression="lzma")  # lossless compression, works very well on masks
    return tiff.imwrite(filename, image)


def write_jp2_image(filename: str, image: np.ndarray) -> None:
    """Write image data to JPEG2000 file

    :param filename: name of JPEG2000 file to write data to
    :param image: image data to write to file
    """
    # Other options:
    # return glymur.Jp2k(filename, data=image)
    # cv2.imwrite(filename, image)
    return write_image(filename, image)


def write_image(filename: str, image: np.ndarray) -> None:
    """Write image data to PNG, JPG file

    :param filename: name of PNG or JPG file to write data to
    :param image: image data to write to file
    """
    data_format = get_data_format(filename)
    if data_format is MimeType.JPG:
        warnings.warn("JPEG is a lossy format therefore saved data will be modified.", category=SHUserWarning)
    return Image.fromarray(image).save(filename)


def write_text(filename: str, data: np.ndarray, add: bool = False) -> None:
    """Write image data to text file

    :param filename: name of text file to write data to
    :param data: image data to write to text file
    :param add: whether to append to existing file or not. Default is `False`
    """
    write_type = "a" if add else "w"
    with open(filename, write_type) as file:
        print(data, end="", file=file)


def write_csv(filename: str, data: np.ndarray, delimiter: str = CSV_DELIMITER) -> None:
    """Write image data to CSV file

    :param filename: name of CSV file to write data to
    :param data: image data to write to CSV file
    :param delimiter: delimiter used in CSV file. Default is ``;``
    """
    with open(filename, "w") as file:
        csv_writer = csv.writer(file, delimiter=delimiter)
        for line in data:
            csv_writer.writerow(line)


def write_json(filename: str, data: Union[list, tuple, dict]) -> None:
    """Write data to JSON file

    :param filename: name of JSON file to write data to
    :param data: data to write to JSON file
    :type data: list, tuple
    """
    with open(filename, "w") as file:
        json.dump(data, file, indent=4, sort_keys=True)


def write_xml(filename: str, element_tree: ElementTree.ElementTree) -> None:
    """Write data to XML or GML file

    :param filename: name of XML or GML file to write data to
    :param element_tree: data as ElementTree object
    :type element_tree: xmlElementTree
    """
    return element_tree.write(filename)
    # this will write declaration tag in first line:
    # return element_tree.write(filename, encoding='utf-8', xml_declaration=True)


def write_numpy(filename: str, data: np.ndarray) -> None:
    """Write data as numpy file

    :param filename: name of numpy file to write data to
    :param data: data to write to numpy file
    :type data: numpy array
    """
    return np.save(filename, data)


def write_bytes(filename: str, data: bytes) -> None:
    """Write binary data into a file

    :param filename: name of file to write the data to
    :param data: binary data to write
    """
    with open(filename, "wb") as file:
        file.write(data)

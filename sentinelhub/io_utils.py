"""
Utility functions to read/write image data from/to file
"""

from __future__ import annotations

import csv
import json
import logging
import os
from typing import IO, Any, Callable, Literal
from xml.etree import ElementTree

import numpy as np
import tifffile as tiff
from PIL import Image

from .constants import MimeType
from .decoding import decode_image_with_pillow, decode_jp2_image, decode_tar, get_data_format

LOGGER = logging.getLogger(__name__)

CSV_DELIMITER = ";"


def read_data(filename: str, data_format: MimeType | None = None) -> Any:
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
        return tiff.imread
    if data_format is MimeType.JP2:
        return _open_file_and_read(decode_jp2_image, "rb")
    if data_format.is_image_format():
        return decode_image_with_pillow

    available_readers: dict[MimeType, Callable[[str], Any]] = {
        MimeType.TAR: _open_file_and_read(decode_tar, "rb"),  # type: ignore[arg-type]
        MimeType.TXT: _open_file_and_read(lambda file: file.read(), "r"),
        MimeType.RAW: _open_file_and_read(lambda file: file.read(), "rb"),
        MimeType.CSV: _read_csv,
        MimeType.JSON: _open_file_and_read(json.load, "rb"),
        MimeType.XML: ElementTree.parse,
        MimeType.GML: ElementTree.parse,
        MimeType.SAFE: ElementTree.parse,
        MimeType.NPY: np.load,
    }

    if data_format not in available_readers:
        raise ValueError(f"Reading data format {data_format} is not supported.")

    return available_readers[data_format]


def _open_file_and_read(reader: Callable[[IO], Any], mode: Literal["r", "rb"]) -> Callable[[str], Any]:
    def new_reader(filename: str) -> Any:
        with open(filename, mode) as file:
            return reader(file)

    return new_reader


def _read_csv(filename: str, delimiter: str = CSV_DELIMITER) -> list:
    """Read data from CSV file

    :param filename: name of CSV file to be read
    :param delimiter: type of CSV delimiter. Default is ``;``
    :return: data stored in CSV file as list
    """
    with open(filename) as file:
        return list(csv.reader(file, delimiter=delimiter))


def write_data(  # noqa: C901
    filename: str, data: Any, data_format: MimeType | None = None, compress: bool = False, add: bool = False
) -> None:
    """Write image data to file

    Function to write image data to specified file. If file format is not provided
    explicitly, it is guessed from the filename extension. If format is TIFF, geo
    information and compression can be optionally added.

    :param filename: name of file to write data to
    :param data: image data to write to file
    :param data_format: format of output file. Default is `None`
    :param compress: Compress data. Default is `False`
    :param add: Append to existing file. Only supported for TXT. Default is `False`
    :raises: exception if numpy format is not supported or file cannot be written
    """
    _create_parent_folder(filename)

    if not isinstance(data_format, MimeType):
        data_format = get_data_format(filename)

    if data_format is MimeType.TIFF:
        tiff.imwrite(filename, data, compression=("lzma" if compress else None))

    elif data_format.is_image_format():
        Image.fromarray(data).save(filename)

    elif data_format is MimeType.NPY:
        np.save(filename, data)

    elif data_format in (MimeType.XML, MimeType.GML):
        data.write(filename)

    elif data_format is MimeType.TXT:
        with open(filename, "a" if add else "w") as file:
            print(data, end="", file=file)

    elif data_format is MimeType.RAW:
        with open(filename, "wb") as file:
            file.write(data)

    elif data_format is MimeType.CSV:
        with open(filename, "w") as file:
            csv_writer = csv.writer(file, delimiter=CSV_DELIMITER)
            for line in data:
                csv_writer.writerow(line)

    elif data_format is MimeType.JSON:
        with open(filename, "w") as file:
            json.dump(data, file, indent=4, sort_keys=True)

    else:
        raise ValueError(f"Writing data format {data_format} is not supported")


def _create_parent_folder(filename: str) -> None:
    path = os.path.dirname(filename)
    if path != "":
        os.makedirs(path, exist_ok=True)

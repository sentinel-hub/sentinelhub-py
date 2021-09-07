"""
Utility functions to read/write image data from/to file
"""

import csv
import json
import os
import logging
import warnings
from xml.etree import ElementTree

import numpy as np
import tifffile as tiff
from PIL import Image

from .decoding import decode_tar, get_data_format, fix_jp2_image, get_jp2_bit_depth
from .constants import MimeType
from .os_utils import create_parent_folder


warnings.simplefilter('ignore', Image.DecompressionBombWarning)
LOGGER = logging.getLogger(__name__)

CSV_DELIMITER = ';'


def read_data(filename, data_format=None):
    """ Read image data from file

    This function reads input data from file. The format of the file
    can be specified in ``data_format``. If not specified, the format is
    guessed from the extension of the filename.

    :param filename: filename to read data from
    :type filename: str
    :param data_format: format of filename. Default is `None`
    :type data_format: MimeType
    :return: data read from filename
    :raises: exception if filename does not exist
    """
    if not os.path.exists(filename):
        raise FileNotFoundError(f'Filename {filename} does not exist')

    if not isinstance(data_format, MimeType):
        data_format = get_data_format(filename)

    reader = _get_reader(data_format)

    try:
        return reader(filename)
    except BaseException as exception:
        # In case a procedure would read a lot of files and one would be corrupt this helps us figure out which one
        LOGGER.debug('Failed to read from file: %s', filename)
        raise exception


def _get_reader(data_format):
    """ Provides a function for reading data in a given data format
    """
    if data_format is MimeType.TIFF:
        return read_tiff_image
    if data_format is MimeType.JP2:
        return read_jp2_image
    if data_format.is_image_format():
        return read_image
    try:
        return {
            MimeType.TAR: read_tar,
            MimeType.TXT: read_text,
            MimeType.RAW: _read_binary,
            MimeType.CSV: read_csv,
            MimeType.JSON: read_json,
            MimeType.XML: read_xml,
            MimeType.GML: read_xml,
            MimeType.SAFE: read_xml
        }[data_format]
    except KeyError as exception:
        raise ValueError(f'Reading data format {data_format} is not supported') from exception


def read_tar(filename):
    """ Read a tar from file
    """
    with open(filename, 'rb') as file:
        return decode_tar(file)


def read_tiff_image(filename):
    """ Read data from TIFF file

    :param filename: name of TIFF file to be read
    :type filename: str
    :return: data stored in TIFF file
    """
    return tiff.imread(filename)


def read_jp2_image(filename):
    """ Read data from JPEG2000 file

    :param filename: name of JPEG2000 file to be read
    :type filename: str
    :return: data stored in JPEG2000 file
    """
    # Other option:
    # return glymur.Jp2k(filename)[:]
    image = read_image(filename)

    with open(filename, 'rb') as file:
        bit_depth = get_jp2_bit_depth(file)

    return fix_jp2_image(image, bit_depth)


def read_image(filename):
    """ Read data from PNG or JPG file

    :param filename: name of PNG or JPG file to be read
    :type filename: str
    :return: data stored in JPG file
    """
    return np.array(Image.open(filename))


def read_text(filename):
    """ Read data from text file

    :param filename: name of text file to be read
    :type filename: str
    :return: data stored in text file
    """
    with open(filename, 'r') as file:
        return file.read()


def _read_binary(filename):
    """ Reads data in bytes
    """
    with open(filename, 'rb') as file:
        return file.read()


def read_csv(filename, delimiter=CSV_DELIMITER):
    """ Read data from CSV file

    :param filename: name of CSV file to be read
    :type filename: str
    :param delimiter: type of CSV delimiter. Default is ``;``
    :type delimiter: str
    :return: data stored in CSV file as list
    """
    with open(filename, 'r') as file:
        return list(csv.reader(file, delimiter=delimiter))


def read_json(filename):
    """ Read data from JSON file

    :param filename: name of JSON file to be read
    :type filename: str
    :return: data stored in JSON file
    """
    with open(filename, 'rb') as file:
        return json.load(file)


def read_xml(filename):
    """ Read data from XML or GML file

    :param filename: name of XML or GML file to be read
    :type filename: str
    :return: data stored in XML file
    """
    return ElementTree.parse(filename)


def read_numpy(filename):
    """ Read data from numpy file

    :param filename: name of numpy file to be read
    :type filename: str
    :return: data stored in file as numpy array
    """
    return np.load(filename)


def write_data(filename, data, data_format=None, compress=False, add=False):
    """ Write image data to file

    Function to write image data to specified file. If file format is not provided
    explicitly, it is guessed from the filename extension. If format is TIFF, geo
    information and compression can be optionally added.

    :param filename: name of file to write data to
    :type filename: str
    :param data: image data to write to file
    :type data: numpy array
    :param data_format: format of output file. Default is `None`
    :type data_format: MimeType
    :param compress: whether to compress data or not. Default is `False`
    :type compress: bool
    :param add: whether to append to existing text file or not. Default is `False`
    :type add: bool
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
        return {
            MimeType.RAW: write_bytes,
            MimeType.CSV: write_csv,
            MimeType.JSON: write_json,
            MimeType.XML: write_xml,
            MimeType.GML: write_xml
        }[data_format](filename, data)
    except KeyError as exception:
        raise ValueError(f'Writing data format {data_format} is not supported') from exception


def write_tiff_image(filename, image, compress=False):
    """ Write image data to TIFF file

    :param filename: name of file to write data to
    :type filename: str
    :param image: image data to write to file
    :type image: numpy array
    :param compress: whether to compress data. If `True`, lzma compression is used. Default is `False`
    :type compress: bool
    """
    if compress:
        return tiff.imsave(filename, image, compress='lzma')  # lossless compression, works very well on masks
    return tiff.imsave(filename, image)


def write_jp2_image(filename, image):
    """ Write image data to JPEG2000 file

    :param filename: name of JPEG2000 file to write data to
    :type filename: str
    :param image: image data to write to file
    :type image: numpy array
    :return: jp2k object
    """
    # Other options:
    # return glymur.Jp2k(filename, data=image)
    # cv2.imwrite(filename, image)
    return write_image(filename, image)


def write_image(filename, image):
    """ Write image data to PNG, JPG file

    :param filename: name of PNG or JPG file to write data to
    :type filename: str
    :param image: image data to write to file
    :type image: numpy array
    """
    data_format = get_data_format(filename)
    if data_format is MimeType.JPG:
        LOGGER.warning('Warning: jpeg is a lossy format therefore saved data will be modified.')
    return Image.fromarray(image).save(filename)


def write_text(filename, data, add=False):
    """ Write image data to text file

    :param filename: name of text file to write data to
    :type filename: str
    :param data: image data to write to text file
    :type data: numpy array
    :param add: whether to append to existing file or not. Default is `False`
    :type add: bool
    """
    write_type = 'a' if add else 'w'
    with open(filename, write_type) as file:
        print(data, end='', file=file)


def write_csv(filename, data, delimiter=CSV_DELIMITER):
    """ Write image data to CSV file

    :param filename: name of CSV file to write data to
    :type filename: str
    :param data: image data to write to CSV file
    :type data: numpy array
    :param delimiter: delimiter used in CSV file. Default is ``;``
    :type delimiter: str
    """
    with open(filename, 'w') as file:
        csv_writer = csv.writer(file, delimiter=delimiter)
        for line in data:
            csv_writer.writerow(line)


def write_json(filename, data):
    """ Write data to JSON file

    :param filename: name of JSON file to write data to
    :type filename: str
    :param data: data to write to JSON file
    :type data: list, tuple
    """
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4, sort_keys=True)


def write_xml(filename, element_tree):
    """ Write data to XML or GML file

    :param filename: name of XML or GML file to write data to
    :type filename: str
    :param element_tree: data as ElementTree object
    :type element_tree: xmlElementTree
    """
    return element_tree.write(filename)
    # this will write declaration tag in first line:
    # return element_tree.write(filename, encoding='utf-8', xml_declaration=True)


def write_numpy(filename, data):
    """ Write data as numpy file

    :param filename: name of numpy file to write data to
    :type filename: str
    :param data: data to write to numpy file
    :type data: numpy array
    """
    return np.save(filename, data)


def write_bytes(filename, data):
    """ Write binary data into a file

    :param filename:
    :param data:
    :return:
    """
    with open(filename, 'wb') as file:
        file.write(data)

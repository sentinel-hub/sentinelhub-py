"""
Module for data decoding
"""
import json
import struct
import tarfile
import warnings
from io import BytesIO
from xml.etree import ElementTree

import numpy as np
import tifffile as tiff
from PIL import Image

from .constants import MimeType
from .exceptions import ImageDecodingError


warnings.simplefilter('ignore', Image.DecompressionBombWarning)


def decode_data(response_content, data_type):
    """ Interprets downloaded data and returns it.

    :param response_content: downloaded data (i.e. json, png, tiff, xml, zip, ... file)
    :type response_content: bytes
    :param data_type: expected downloaded data type
    :type data_type: constants.MimeType
    :return: downloaded data
    :rtype: numpy array in case of image data type, or other possible data type
    :raises: ValueError
    """
    if data_type is MimeType.JSON:
        response_text = response_content.decode('utf-8')
        if not response_text:
            return response_text
        return json.loads(response_text)
    if data_type is MimeType.TAR:
        return decode_tar(response_content)
    if MimeType.is_image_format(data_type):
        return decode_image(response_content, data_type)
    if data_type is MimeType.XML or data_type is MimeType.GML or data_type is MimeType.SAFE:
        return ElementTree.fromstring(response_content)

    try:
        return {
            MimeType.RAW: response_content,
            MimeType.TXT: response_content,
            MimeType.ZIP: BytesIO(response_content)
        }[data_type]
    except KeyError as exception:
        raise ValueError(f'Decoding data format {data_type} is not supported') from exception


def decode_image(data, image_type):
    """ Decodes the image provided in various formats, i.e. png, 16-bit float tiff, 32-bit float tiff, jp2
    and returns it as an numpy array

    :param data: image in its original format
    :type data: any of possible image types
    :param image_type: expected image format
    :type image_type: constants.MimeType
    :return: image as numpy array
    :rtype: numpy array
    :raises: ImageDecodingError
    """
    bytes_data = BytesIO(data)
    if image_type is MimeType.TIFF:
        image = tiff.imread(bytes_data)
    else:
        image = np.array(Image.open(bytes_data))

        if image_type is MimeType.JP2:
            try:
                bit_depth = get_jp2_bit_depth(bytes_data)
                image = fix_jp2_image(image, bit_depth)
            except ValueError:
                pass

    if image is None:
        raise ImageDecodingError('Unable to decode image')
    return image


def decode_tar(data):
    """ A decoder to convert response bytes into a dictionary of {filename: value}

    :param data: Data to decode
    :type data: bytes or IOBase
    :return: A dictionary of decoded files from a tar file
    :rtype: dict(str: object)
    """
    if isinstance(data, bytes):
        data = BytesIO(data)

    with tarfile.open(fileobj=data) as tar:
        file_members = (member for member in tar.getmembers() if member.isfile())
        itr = ((member.name, get_data_format(member.name), tar.extractfile(member)) for member in file_members)
        return {filename: decode_data(file.read(), file_type) for filename, file_type, file in itr}


def decode_sentinelhub_err_msg(response):
    """ Decodes error message from Sentinel Hub service

    :param response: Sentinel Hub service response
    :type response: requests.Response
    :return: An error message
    :rtype: str
    """
    try:
        server_message = []
        for elem in decode_data(response.content, MimeType.XML):
            if 'ServiceException' in elem.tag or 'Message' in elem.tag:
                server_message.append(elem.text.strip('\n\t '))
        return ''.join(server_message)
    except ElementTree.ParseError:
        return response.text


def get_jp2_bit_depth(stream):
    """ Reads bit encoding depth of jpeg2000 file in binary stream format

    :param stream: binary stream format
    :type stream: Binary I/O (e.g. io.BytesIO, io.BufferedReader, ...)
    :return: bit depth
    :rtype: int
    """
    stream.seek(0)
    while True:
        read_buffer = stream.read(8)
        if len(read_buffer) < 8:
            raise ValueError('Image Header Box not found in Jpeg2000 file')

        _, box_id = struct.unpack('>I4s', read_buffer)

        if box_id == b'ihdr':
            read_buffer = stream.read(14)
            params = struct.unpack('>IIHBBBB', read_buffer)
            return (params[3] & 0x7f) + 1


def fix_jp2_image(image, bit_depth):
    """ Because Pillow library incorrectly reads JPEG 2000 images with 15-bit encoding this function corrects the
    values in image.

    :param image: image read by opencv library
    :type image: numpy array
    :param bit_depth: bit depth of jp2 image encoding
    :type bit_depth: int
    :return: corrected image
    :rtype: numpy array
    """
    if bit_depth in [8, 16]:
        return image
    if bit_depth == 15:
        try:
            return image >> 1
        except TypeError as exception:
            raise IOError('Failed to read JPEG 2000 image correctly. Most likely reason is that Pillow did not '
                          'install OpenJPEG library correctly. Try reinstalling Pillow from a wheel') from exception

    raise ValueError(f'Bit depth {bit_depth} of jp2 image is currently not supported. '
                     'Please raise an issue on package Github page')


def get_data_format(filename):
    """ Util function to guess format from filename extension

    :param filename: name of file
    :type filename: str
    :return: file extension
    :rtype: MimeType
    """
    fmt_ext = filename.split('.')[-1]
    return MimeType.from_string(fmt_ext)

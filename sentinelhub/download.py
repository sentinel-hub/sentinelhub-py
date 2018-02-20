"""
Module for downloading data
"""

# pylint: disable=too-many-instance-attributes

import logging
import os
import time
import concurrent.futures
import requests
import json
import cv2
import numpy as np
import tifffile as tiff
from io import BytesIO
from xml.etree import ElementTree

from .os_utils import create_parent_folder
from .constants import MimeType, RequestType
from .config import SGConfig
from .io_utils import get_jp2_bit_depth, _fix_jp2_image

LOGGER = logging.getLogger(__name__)


class DownloadFailedException(Exception):
    pass


class ImageDecodingError(Exception):
    pass


class DownloadRequest:
    """ Class to manage HTTP requests

    Container for all download requests issued by the DataRequests containing
    url to Sentinel Hub's services or other sources to data, file names to
    saved data and other necessary flags needed when the data is downloaded and
    interpreted.

    :param url: url to Sentinel Hub's services or other sources from where the data is downloaded. Default is ``None``
    :type url: str
    :param data_folder: folder name where the fetched data will be (or already is) saved. Default is ``None``
    :type data_folder: str
    :param filename: filename of the file where the fetched data will be (or already is) saved. Default is ``None``
    :type filename: str
    :param headers: add HTTP headers to request. Default is ``None``
    :type headers: dict
    :param request_type: type of request, either GET or POST. Default is ``constants.RequestType.GET``
    :type request_type: constants.RequestType
    :param post_values: form encoded data to send in POST request. Default is ``None``
    :type post_values: dict
    :param save_response: flag to turn on/off saving data downloaded by this request to disk. Default is ``True``.
    :type save_response: bool
    :param return_data: flag to return or not data downloaded by this request to disk. Default is ``True``.
    :type return_data: bool
    :param data_type: expected file format of downloaded data. Default is ``constants.MimeType.RAW``
    :type data_type: constants.MimeType

    """
    def __init__(self, *, url=None, data_folder=None, filename=None, headers=None, request_type=RequestType.GET,
                 post_values=None, save_response=True, return_data=True, data_type=MimeType.RAW, **properties):

        self.url = url
        self.data_folder = data_folder
        self.filename = filename
        self.headers = headers
        self.post_values = post_values
        self.save_response = save_response
        self.return_data = return_data

        self.properties = properties

        self.request_type = RequestType(request_type)
        self.data_type = MimeType(data_type)

        self.will_download = True
        self.file_location = None
        self._set_file_location()

    def set_save_response(self, save_response):
        """
        Set save_response attribute

        :param save_response: flag to turn on/off saving data downloaded by this request to disk. Default is ``True``.
        :return: bool
        """
        self.save_response = save_response

    def set_return_data(self, return_data):
        """
        Set return_data attribute

        :param return_data: flag to return or not data downloaded by this request to disk. Default is ``True``.
        :return: bool
        """
        self.return_data = return_data

    def set_data_folder(self, data_folder):
        """
        Set data_folder attribute

        :param data_folder: folder name where the fetched data will be (or already is) saved.
        :return: str
        """
        self.data_folder = data_folder
        self._set_file_location()

    def _set_file_location(self):
        if self.data_folder is not None and self.filename is not None:
            self.file_location = os.path.join(self.data_folder, self.filename.lstrip('/'))

    def is_downloaded(self):
        """ Checks if data for this request has already been downloaded and is saved to disk.

        :return: returns ``True`` if data for this request has already been downloaded and is saved to disk.
        :rtype: bool
        """
        if self.data_folder is None:
            return False
        self._set_file_location()
        return os.path.exists(self.file_location)

    def get_filename(self):
        """ Returns the full filename.

        :return: full filename (data folder + filename)
        :rtype: str
        """
        if self.data_folder:
            return os.path.join(self.data_folder, self.filename)
        return None


def download_data(request_list, redownload=False, max_threads=None):
    """ Download all requested data or read data from disk, if already downloaded and available and redownload is
    not required.

    :param request_list: list of DownloadRequests
    :type request_list: list of DownloadRequests
    :param redownload: if ``True``, download again the data, although it was already downloaded and is available
                        on the disk. Default is ``False``.
    :type redownload: bool
    :param max_threads: number of threads to use. Default is ``max_threads=None`` (``5*N`` where ``N`` = nr. of cores on
                        the system)
    :type max_threads: int
    :return: list of Futures holding downloaded data, where each element in the list corresponds to an element
                in the download request list.
    :rtype: list[concurrent.futures.Future]
    """
    _check_if_must_download(request_list, redownload)

    LOGGER.debug("Using max_threads=%s for %s requests", max_threads, len(request_list))

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        return [executor.submit(execute_download_request, request) for request in request_list]


def _check_if_must_download(request_list, redownload):
    """
    Updates request.will_download attribute of each request in request_list.

    **Note:** the function mutates the elements of the list!

    :param request_list: a list of ``DownloadRequest`` instances
    :type: list[DownloadRequest]
    :param redownload: tells whether to download the data again or not
    :type: bool
    """
    for request in request_list:
        request.will_download = (request.save_response or request.return_data) \
                                and (not request.is_downloaded() or redownload)


def execute_download_request(request):
    """ Executes download request.

    :param request: DownloadRequest to be executed
    :type request: DownloadRequest
    :return: downloaded data or None
    :rtype: numpy array, other possible data type or None
    :raises: DownloadFailedException
    """
    if request.save_response and request.data_folder is None:
        raise ValueError('Data folder is not specified. '
                         'Please give a data folder name in the initialisation of your request.')

    if not request.will_download:
        return None

    try_num = SGConfig().max_download_attempts
    response = None
    while try_num > 0:
        try:
            response = _do_request(request)
            response.raise_for_status()
            LOGGER.debug('Successful download from %s', request.url)
            break
        except requests.RequestException as exception:
            try_num -= 1
            if try_num > 0 and _is_temporal_problem(exception) or\
                    (isinstance(exception, requests.HTTPError) and
                     exception.response.status_code != requests.status_codes.codes.BAD_REQUEST):
                LOGGER.debug('Download attempt failed: %s\n%d attempts left, will retry in %ds', exception,
                             try_num, SGConfig().download_sleep_time)
                time.sleep(SGConfig().download_sleep_time)
            else:
                raise DownloadFailedException(_create_download_failed_message(exception))

    _save_if_needed(request, response)

    if request.return_data:
        return decode_data(response, request.data_type)
    return None


def _do_request(request):
    """ Executes download request
    :param request: A request
    :type request: DownloadRequest
    :return: Response of the request
    """
    if request.request_type is RequestType.GET:
        return requests.get(request.url, headers=request.headers)
    elif request.request_type is RequestType.POST:
        return requests.post(request.url, data=json.dumps(request.post_values), headers=request.headers)
    raise ValueError('Invalid request type {}'.format(request.request_type))


def _is_temporal_problem(exception):
    """ Checks if the obtained exception is temporal and if download attempt should be repeated

    :param exception: Exception raised during download
    :type exception: Exception
    :return: True if exception is temporal and False otherwise
    :rtype: bool
    """
    return isinstance(exception, (requests.ConnectionError, requests.ConnectTimeout, requests.ReadTimeout,
                                  requests.Timeout))


def _create_download_failed_message(exception):
    """ Creates message describing why download has failed

    :param exception: Exception raised during download
    :type exception: Exception
    :return: Error message
    :rtype: str
    """
    message = 'Failed to download with {}:\n{}'.format(exception.__class__.__name__, exception)

    if _is_temporal_problem(exception):
        if isinstance(exception, requests.ConnectionError):
            message += '\nPlease check your internet connection and try again.'
        else:
            message += '\nThere might be a problem in connection or the server failed to process ' \
                       'your request. Please try again.'
    elif isinstance(exception, requests.HTTPError):
        try:
            server_message = ''
            for elem in decode_data(exception.response, MimeType.XML):
                if 'ServiceException' in elem.tag:
                    server_message += elem.text.strip('\n\t ')
        except ElementTree.ParseError:
            server_message = exception.response.text
        message += '\nServer response: "{}"'.format(server_message)

    return message


def _save_if_needed(request, response):
    """ Save data to disk, if requested by the user
    :param request: Download request
    :type: DownloadRequest
    :param response: Response object from requests module
    """
    if request.save_response:
        create_parent_folder(request.file_location)
        with open(request.file_location, 'wb') as file:
            file.write(response.content)
        LOGGER.debug('Saved data from %s to %s', request.url, request.file_location)


def decode_data(response, data_type):
    """ Interprets downloaded data and returns it.

    :param response: downloaded data (i.e. json, png, tiff, xml, zip, ... file)
    :type response: requests.models.Response object
    :param data_type: expected downloaded data type
    :type data_type: constants.MimeType
    :return: downloaded data
    :rtype: numpy array in case of image data type, or other possible data type
    :raises: ValueError
    """
    LOGGER.debug('data_type=%s', data_type)

    if data_type is MimeType.JSON:
        return response.json()
    elif MimeType.is_image_format(data_type):
        return decode_image(response.content, data_type)
    elif data_type is MimeType.RAW or data_type is MimeType.TXT:
        return response.content
    elif data_type is MimeType.REQUESTS_RESPONSE:
        return response
    elif data_type is MimeType.ZIP:
        return BytesIO(response.content)
    elif data_type is MimeType.XML or data_type is MimeType.GML or \
            data_type is MimeType.SAFE:
        return ElementTree.fromstring(response.content)

    raise ValueError('Unknown response data type {}'.format(data_type))


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
    if image_type is MimeType.TIFF or image_type is MimeType.TIFF_d32f:
        image = tiff.imread(BytesIO(data))
    else:
        img_array = np.asarray(bytearray(data))
        image = cv2.imdecode(img_array, cv2.IMREAD_UNCHANGED)

        if image_type is MimeType.JP2:
            bit_depth = get_jp2_bit_depth(BytesIO(data))
            image = _fix_jp2_image(image, bit_depth)

    if image is None:
        raise ImageDecodingError('Unable to decode image')
    return image


def get_json(url, post_values=None, headers=None):
    """ Download request as JSON data type

    :param url: url to Sentinel Hub's services or other sources from where the data is downloaded
    :type url: str
    :param post_values: form encoded data to send in POST request. Default is ``None``
    :type post_values: dict
    :param headers: add HTTP headers to request. Default is ``None``
    :type headers: dict
    :return: request response as JSON instance
    :rtype: JSON instance or None
    :raises: RunTimeError
    """

    json_headers = {} if headers is None else headers.copy()

    if post_values is None:
        request_type = RequestType.GET
    else:
        request_type = RequestType.POST
        json_headers = {**json_headers, **{'Content-Type': MimeType.get_string(MimeType.JSON)}}

    request = DownloadRequest(url=url, headers=json_headers, request_type=request_type, post_values=post_values,
                              save_response=False, return_data=True, data_type=MimeType.JSON)

    return execute_download_request(request)


def get_xml(url):
    """ Download request as XML data type

    :param url: url to Sentinel Hub's services or other sources from where the data is downloaded
    :type url: str
    :return: request response as XML instance
    :rtype: XML instance or None
    :raises: RunTimeError
    """
    request = DownloadRequest(url=url, request_type=RequestType.GET, save_response=False, return_data=True,
                              data_type=MimeType.XML)

    return execute_download_request(request)

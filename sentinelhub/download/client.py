"""
Module implementing the main download client class
"""
import concurrent.futures
import logging
import warnings
import os
import sys

import requests

from ..config import SHConfig
from ..constants import RequestType, MimeType
from ..decoding import decode_data
from ..exceptions import DownloadFailedException, SHRuntimeWarning
from ..io_utils import LocalIO
from .handlers import fail_user_errors, retry_temporal_errors
from .request import DownloadRequest
from .cache import hash_request


LOGGER = logging.getLogger(__name__)


class DownloadClient:
    """ The download client class which takes care of downloading the data, parallelizing the download through threads
    and catching any exceptions.
    """
    def __init__(self, *, redownload=False, raise_download_errors=True, io_object=None, config=None):
        """
        :param redownload: If `True` the data will always be downloaded again. By default this is set to `False` and
            the data that has already been downloaded and saved to an expected location will be read from the
            location instead of being downloaded again.
        :type redownload: bool
        :param raise_download_errors: If `True` any error in download process will be raised as
            `DownloadFailedException`. If `False` failed downloads will only raise warnings.
        :type raise_download_errors: bool
        :param io_object: An instance of IO class
        :type io_object: LocalIO
        :param config: An instance of configuration class
        :type config: SHConfig
        """
        self.redownload = redownload
        self.raise_download_errors = raise_download_errors

        self.io_object = LocalIO() if io_object is None else io_object

        self.config = SHConfig() if config is None else config

    def download(self, download_requests, max_threads=None):
        """ Download one or multiple requests, provided as a request list.

        :param download_requests: A list of requests or a single request to be executed.
        :type download_requests: List[DownloadRequest] or DownloadRequest
        :param max_threads: Maximum number of threads to be used for the download.
        :type max_threads: int
        :return: A list of results or a single result, depending on input parameter `requests`
        :rtype: list(object) or object
        """
        is_single_request = isinstance(download_requests, DownloadRequest)
        if is_single_request:
            download_requests = [download_requests]

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            download_list = [executor.submit(self._single_download, request) for request in download_requests]

        data_list = []
        for future in download_list:
            try:
                data_list.append(future.result())
            except DownloadFailedException as download_exception:
                if self.raise_download_errors:
                    traceback = sys.exc_info()[2]
                    raise download_exception.with_traceback(traceback)

                warnings.warn(str(download_exception), category=SHRuntimeWarning)
                data_list.append(None)

        if is_single_request:
            return data_list[0]
        return data_list

    def _single_download(self, request):
        """ Method for downloading a single request

        :param request: An object with information about download and storage of data
        :type request: DownloadRequest
        :return: Downloaded data
        :rtype: object
        """
        request.raise_if_invalid()

        if request.save_response:
            request.file_path = os.path.join(request.data_folder, request.file_path)
        elif request.hash_save:
            hashed, hashable = hash_request(request.url, request.post_values)
            folder_path = os.path.join(request.data_folder, hashed)
            request.file_path = os.path.join(folder_path, 'response.{}'.format(request.data_type.value))
            request_path = os.path.join(folder_path, 'request.json')
            LOGGER.debug('Saving hashed request to %s', request_path)
            self.io_object.write(request_path, hashable, data_format=MimeType.TXT)

        if not self.is_download_required(request):
            if request.return_data:
                return self.io_object.read(request.file_path, data_format=request.data_type)
            return None

        response_content = self._execute_download(request)

        if request.save_response or request.hash_save:
            self.io_object.write(request.file_path, response_content, data_format=MimeType.RAW)
            LOGGER.debug('Saved data to %s', request.file_path)

        if request.return_data:
            return decode_data(response_content, request.data_type)
        return None

    @retry_temporal_errors
    @fail_user_errors
    def _execute_download(self, request):
        response = requests.request(
            request.request_type.value,
            url=request.url,
            json=request.post_values,
            headers=request.headers,
            timeout=self.config.download_timeout_seconds
        )

        response.raise_for_status()
        LOGGER.debug('Successful download from %s', request.url)

        return response.content

    def is_download_required(self, request):
        """ Checks if download should actually be done

        :param request: An object with information about download and storage of data
        :type request: DownloadRequest
        :return: True if download should be done and False otherwise
        :rtype: bool
        """
        val = (request.save_response or request.return_data) and \
              (self.redownload or request.file_path is None or not self.io_object.exists(request.file_path))
        return val


def get_json(url, post_values=None, headers=None, download_client_class=DownloadClient):
    """ Download request as JSON data type

    :param url: url to Sentinel Hub's services or other sources from where the data is downloaded
    :type url: str
    :param post_values: form encoded data to send in POST request. Default is `None`
    :type post_values: dict
    :param headers: add HTTP headers to request. Default is `None`
    :type headers: dict
    :return: request response as JSON instance
    :rtype: JSON instance or None
    :raises: RunTimeError
    """
    json_headers = {} if headers is None else headers

    if post_values is None:
        request_type = RequestType.GET
    else:
        request_type = RequestType.POST
        json_headers = {**json_headers, **{'Content-Type': MimeType.JSON.get_string()}}

    request = DownloadRequest(url=url, headers=json_headers, request_type=request_type, post_values=post_values,
                              save_response=False, return_data=True, data_type=MimeType.JSON)

    return download_client_class().download(request)


def get_xml(url, download_client_class=DownloadClient):
    """ Download request as XML data type

    :param url: url to Sentinel Hub's services or other sources from where the data is downloaded
    :type url: str
    :return: request response as XML instance
    :rtype: XML instance or None
    :raises: RunTimeError
    """
    request = DownloadRequest(url=url, request_type=RequestType.GET, save_response=False, return_data=True,
                              data_type=MimeType.XML)

    return download_client_class().download(request)

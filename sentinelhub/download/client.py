"""
Module implementing the main download client class
"""
import logging
import warnings
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from tqdm.auto import tqdm

from ..config import SHConfig
from ..constants import RequestType, MimeType
from ..decoding import decode_data as decode_data_function
from ..exceptions import DownloadFailedException, SHRuntimeWarning, HashedNameCollisionException
from ..io_utils import read_data, write_data
from .handlers import fail_user_errors, retry_temporary_errors
from .request import DownloadRequest


LOGGER = logging.getLogger(__name__)


class DownloadClient:
    """ A basic download client object

    It does the following:

    - downloads the data with multiple threads in parallel,
    - handles any exceptions that occur during download,
    - decodes downloaded data,
    - reads and writes locally stored/cached data
    """
    def __init__(self, *, redownload=False, raise_download_errors=True, config=None):
        """
        :param redownload: If `True` the data will always be downloaded again. By default this is set to `False` and
            the data that has already been downloaded and saved to an expected location will be read from the
            location instead of being downloaded again.
        :type redownload: bool
        :param raise_download_errors: If `True` any error in download process will be raised as
            `DownloadFailedException`. If `False` failed downloads will only raise warnings.
        :type raise_download_errors: bool
        :param config: An instance of configuration class
        :type config: SHConfig
        """
        self.redownload = redownload
        self.raise_download_errors = raise_download_errors

        self.config = config or SHConfig()

    def download(self, download_requests, max_threads=None, decode_data=True, show_progress=False):
        """ Download one or multiple requests, provided as a request list.

        :param download_requests: A list of requests or a single request to be executed.
        :type download_requests: List[DownloadRequest] or DownloadRequest
        :param max_threads: Maximum number of threads to be used for download in parallel. The default is
            `max_threads=None` which will use the number of processors on the system multiplied by 5.
        :type max_threads: int or None
        :param decode_data: If `True` it will decode data otherwise it will return it in binary format.
        :type decode_data: bool
        :param show_progress: Whether a progress bar should be displayed while downloading
        :type show_progress: bool
        :return: A list of results or a single result, depending on input parameter `download_requests`
        :rtype: list(object) or object
        """
        is_single_request = isinstance(download_requests, DownloadRequest)
        if is_single_request:
            download_requests = [download_requests]

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            download_list = [
                executor.submit(self._single_download, request, decode_data) for request in download_requests
            ]
            future_order = {future: i for i, future in enumerate(download_list)}

        data_list = [None] * len(download_list)
        # Consider using tqdm.contrib.concurrent.thread_map in the future
        if show_progress:
            with tqdm(total=len(download_list)) as pbar:
                for future in as_completed(download_list):
                    data_list[future_order[future]] = self._process_download_future(future)
                    pbar.update(1)
        else:
            for future in as_completed(download_list):
                data_list[future_order[future]] = self._process_download_future(future)

        if is_single_request:
            return data_list[0]
        return data_list

    def _process_download_future(self, future):
        """ Unpacks the future and correctly handles exceptions
        """
        try:
            return future.result()
        except DownloadFailedException as download_exception:
            if self.raise_download_errors:
                traceback = sys.exc_info()[2]
                raise download_exception.with_traceback(traceback)

            warnings.warn(str(download_exception), category=SHRuntimeWarning)
            return None

    def _single_download(self, request, decode_data):
        """ Method for downloading a single request
        """
        request.raise_if_invalid()

        request_path, response_path = request.get_storage_paths()

        if not self._is_download_required(request, response_path):
            if request.return_data:
                LOGGER.debug('Reading locally stored data from %s instead of downloading', response_path)
                self._check_cached_request_is_matching(request, request_path)
                return read_data(response_path, data_format=request.data_type if decode_data else MimeType.RAW)
            return None

        response_content = self._execute_download(request)

        if request_path and request.save_response and (self.redownload or not os.path.exists(request_path)):
            request_info = request.get_request_params(include_metadata=True)
            write_data(request_path, request_info, data_format=MimeType.JSON)
            LOGGER.debug('Saved request info to %s', request_path)

        if request.save_response:
            write_data(response_path, response_content, data_format=MimeType.RAW)
            LOGGER.debug('Saved data to %s', response_path)

        if request.return_data:
            if decode_data:
                return decode_data_function(response_content, request.data_type)
            return response_content
        return None

    @retry_temporary_errors
    @fail_user_errors
    def _execute_download(self, request):
        """ A default way of executing a single download request
        """
        LOGGER.debug('Sending %s request to %s. Hash of sent request is %s',
                     request.request_type.value, request.url, request.get_hashed_name())

        response = requests.request(
            request.request_type.value,
            url=request.url,
            json=request.post_values,
            headers=request.headers,
            timeout=self.config.download_timeout_seconds
        )

        response.raise_for_status()
        LOGGER.debug('Successful %s request to %s', request.request_type.value, request.url)

        return response.content

    def _is_download_required(self, request, response_path):
        """ Checks if download should actually be done
        """
        return (request.save_response or request.return_data) and \
               (self.redownload or response_path is None or not os.path.exists(response_path))

    @staticmethod
    def _check_cached_request_is_matching(request, request_path):
        """ Ensures that the cached request matches the current one. Serves as protection against hash collisions
        """
        cached_request_info = read_data(request_path, MimeType.JSON)
        current_request_info = request.get_request_params(include_metadata=False)
        # Timestamps are allowed to differ
        del cached_request_info['timestamp']
        del cached_request_info['headers']

        if cached_request_info != current_request_info:
            raise HashedNameCollisionException(
                f'Request has hashed name {request.get_hashed_name()}, which matches request saved at {request_path}, '
                f'but the requests are different. Possible hash collision'
            )

    def get_json(self, url, post_values=None, headers=None, request_type=None, **kwargs):
        """ Download request as JSON data type

        :param url: An URL from where the data will be downloaded
        :type url: str
        :param post_values: A dictionary of parameters for a POST request
        :type post_values: dict or None
        :param headers: A dictionary of additional request headers
        :type headers: dict
        :param request_type: A type of HTTP request to make. If not specified, then it will be a GET request if
            `post_values=None` and a POST request otherwise
        :type request_type: RequestType or None
        :param kwargs: Any other parameters that are passed to DownloadRequest class
        :return: JSON data parsed into Python objects
        :rtype: dict or list or str or None
        """
        json_headers = headers or {}

        if request_type is None:
            request_type = RequestType.GET if post_values is None else RequestType.POST

        if request_type is RequestType.POST and 'Content-Type' not in json_headers:
            json_headers = {
                'Content-Type': MimeType.JSON.get_string(),
                **json_headers
            }

        request = DownloadRequest(url=url, headers=json_headers, request_type=request_type, post_values=post_values,
                                  data_type=MimeType.JSON, **kwargs)

        return self._single_download(request, decode_data=True)

    def get_xml(self, url, **kwargs):
        """ Download request as XML data type

        :param url: url to Sentinel Hub's services or other sources from where the data is downloaded
        :type url: str
        :param kwargs: Any other parameters that are passed to DownloadRequest class
        :return: request response as XML instance
        :rtype: XML instance or None
        """
        request = DownloadRequest(url=url, data_type=MimeType.XML, **kwargs)
        return self._single_download(request, decode_data=True)

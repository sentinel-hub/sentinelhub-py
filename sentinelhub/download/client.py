"""
Module implementing the main download client class
"""
import json
import logging
import os
import sys
import warnings
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Any, List, Optional, Union, overload
from xml.etree import ElementTree

import requests
from tqdm.auto import tqdm

from ..config import SHConfig
from ..constants import MimeType, RequestType
from ..exceptions import (
    DownloadFailedException,
    HashedNameCollisionException,
    MissingDataInRequestException,
    SHRuntimeWarning,
)
from ..io_utils import read_data
from ..type_utils import JsonDict
from .handlers import fail_user_errors, retry_temporary_errors
from .models import DownloadRequest, DownloadResponse

LOGGER = logging.getLogger(__name__)


class DownloadClient:
    """A basic download client object

    It does the following:

    - downloads the data with multiple threads in parallel,
    - handles any exceptions that occur during download,
    - decodes downloaded data,
    - reads and writes locally stored/cached data
    """

    def __init__(
        self, *, redownload: bool = False, raise_download_errors: bool = True, config: Optional[SHConfig] = None
    ):
        """
        :param redownload: If `True` the data will always be downloaded again. By default, this is set to `False` and
            the data that has already been downloaded and saved to an expected location will be read from the
            location instead of being downloaded again.
        :param raise_download_errors: If `True` any error in download process will be raised as
            `DownloadFailedException`. If `False` failed downloads will only raise warnings.
        :param config: An instance of configuration class
        """
        self.redownload = redownload
        self.raise_download_errors = raise_download_errors

        self.config = config or SHConfig()

    @overload
    def download(
        self,
        download_requests: DownloadRequest,
        max_threads: Optional[int] = None,
        decode_data: bool = True,
        show_progress: bool = False,
    ) -> Any:
        ...

    @overload
    def download(
        self,
        download_requests: List[DownloadRequest],
        max_threads: Optional[int] = None,
        decode_data: bool = True,
        show_progress: bool = False,
    ) -> List[Any]:
        ...

    def download(
        self,
        download_requests: Union[DownloadRequest, List[DownloadRequest]],
        max_threads: Optional[int] = None,
        decode_data: bool = True,
        show_progress: bool = False,
    ) -> Union[List[Any], Any]:
        """Download one or multiple requests, provided as a request list.

        :param download_requests: A list of requests or a single request to be executed.
        :param max_threads: Maximum number of threads to be used for download in parallel. The default is
            `max_threads=None` which will use the number of processors on the system multiplied by 5.
        :param decode_data: If `True` it will decode data otherwise it will return it in form of a `DownloadResponse`
            objects which contain binary data and response metadata.
        :param show_progress: Whether a progress bar should be displayed while downloading
        :return: A list of results or a single result, depending on input parameter `download_requests`
        """
        downloads = [download_requests] if isinstance(download_requests, DownloadRequest) else download_requests

        data_list = [None] * len(downloads)

        single_download_method = self._single_download_decoded if decode_data else self._single_download
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            download_list = [executor.submit(single_download_method, request) for request in downloads]
            future_order = {future: i for i, future in enumerate(download_list)}

            # Consider using tqdm.contrib.concurrent.thread_map in the future
            if show_progress:
                with tqdm(total=len(download_list)) as pbar:
                    for future in as_completed(download_list):
                        data_list[future_order[future]] = self._process_download_future(future)
                        pbar.update(1)
            else:
                for future in as_completed(download_list):
                    data_list[future_order[future]] = self._process_download_future(future)

        if isinstance(download_requests, DownloadRequest):
            return data_list[0]
        return data_list

    def _process_download_future(self, future: Future) -> Any:
        """Unpacks the future and correctly handles exceptions"""
        try:
            return future.result()
        except DownloadFailedException as download_exception:
            if self.raise_download_errors:
                traceback = sys.exc_info()[2]
                raise download_exception.with_traceback(traceback)

            warnings.warn(str(download_exception), category=SHRuntimeWarning)
            return None

    def _single_download_decoded(self, request: DownloadRequest) -> Any:
        """Downloads a response and decodes it into data. By decoding a single response"""
        response = self._single_download(request)
        return None if response is None else response.decode()

    def _single_download(self, request: DownloadRequest) -> Optional[DownloadResponse]:
        """Method for downloading a single request."""
        request.raise_if_invalid()
        if not (request.save_response or request.return_data):
            return None

        request_path, response_path = request.get_storage_paths()

        no_local_data = self.redownload or response_path is None or not os.path.exists(response_path)
        if no_local_data:
            response = self._execute_download(request)
        else:
            if not request.return_data or response_path is None:
                return None

            LOGGER.debug("Reading locally stored data from %s instead of downloading", response_path)
            self._check_cached_request_is_matching(request, request_path)
            response = DownloadResponse.from_local(request)

        processed_response = self._process_response(request, response)

        if request.save_response and response_path and (no_local_data or processed_response is not response):
            processed_response.to_local()
            LOGGER.debug("Saved response data to %s", response_path)

        if request.return_data:
            return processed_response
        return None

    @retry_temporary_errors
    @fail_user_errors
    def _execute_download(self, request: DownloadRequest) -> DownloadResponse:
        """A default way of executing a single download request"""
        if request.url is None:
            raise ValueError(f"Faulty request {request}, no URL specified.")

        LOGGER.debug(
            "Sending %s request to %s. Hash of sent request is %s",
            request.request_type.value,
            request.url,
            request.get_hashed_name(),
        )

        response = requests.request(
            request.request_type.value,
            url=request.url,
            json=request.post_values,
            headers=request.headers,
            timeout=self.config.download_timeout_seconds,
        )

        response.raise_for_status()
        LOGGER.debug("Successful %s request to %s", request.request_type.value, request.url)

        return DownloadResponse.from_response(response, request)

    @staticmethod
    def _check_cached_request_is_matching(request: DownloadRequest, request_path: Optional[str]) -> None:
        """Ensures that the cached request matches the current one. Serves as protection against hash collisions"""
        if not request_path:
            return

        cached_request_info = read_data(request_path, MimeType.JSON)
        # Backwards compatibility - older versions don't have "request" subdict
        cached_request_info = cached_request_info.get("request", cached_request_info)

        # Timestamps are allowed to differ
        del cached_request_info["timestamp"]
        del cached_request_info["headers"]

        current_request_info = request.get_request_params(include_metadata=False)
        # Saved request was jsonified
        current_request_info_json = json.loads(json.dumps(current_request_info))

        if cached_request_info != current_request_info_json:
            raise HashedNameCollisionException(
                f"Request has hashed name {request.get_hashed_name()}, which matches request saved at {request_path}, "
                "but the requests are different. Possible hash collision"
            )

    def _process_response(self, _: DownloadRequest, response: DownloadResponse) -> DownloadResponse:
        """This method is meant to be overwritten by inherited implementations of the client object."""
        return response

    def get_json(
        self,
        url: str,
        post_values: Optional[JsonDict] = None,
        headers: Optional[JsonDict] = None,
        request_type: Optional[RequestType] = None,
        **kwargs: Any,
    ) -> Union[JsonDict, list, str, None]:
        """Download request as JSON data type

        :param url: A URL from where the data will be downloaded
        :param post_values: A dictionary of parameters for a POST request
        :param headers: A dictionary of additional request headers
        :param request_type: A type of HTTP request to make. If not specified, then it will be a GET request if
            `post_values=None` and a POST request otherwise
        :param kwargs: Any other parameters that are passed to DownloadRequest class
        :return: JSON data parsed into Python objects
        """
        json_headers = headers or {}

        if request_type is None:
            request_type = RequestType.GET if post_values is None else RequestType.POST

        if request_type is RequestType.POST and "Content-Type" not in json_headers:
            json_headers = {"Content-Type": MimeType.JSON.get_string(), **json_headers}

        request = DownloadRequest(
            url=url,
            headers=json_headers,
            request_type=request_type,
            post_values=post_values,
            data_type=MimeType.JSON,
            **kwargs,
        )

        return self._single_download_decoded(request)

    def get_json_dict(self, url: str, *args: Any, extract_key: Optional[str] = None, **kwargs: Any) -> JsonDict:
        """Download request as JSON data type, failing if the result is not a dictionary

        For other parameters see `get_json` method.

        :param url: A URL from where the data will be downloaded
        :param extract_key: If provided, the field is automatically extracted, checked, and returned
        """
        response = self.get_json(url, *args, **kwargs)

        if not isinstance(response, dict):
            raise MissingDataInRequestException(
                f"Response from {url} was expected to be a dictionary, but got {type(response)}."
            )

        if extract_key is None:
            return response
        if extract_key in response and isinstance(response[extract_key], dict):
            return response[extract_key]

        explanation = f"Value is of type {type(response[extract_key])}" if extract_key in response else "Key is missing"
        raise MissingDataInRequestException(
            f"Response from {url} was expected to have {extract_key} mapping to a dictionary. {explanation}."
        )

    def get_xml(self, url: str, **kwargs: Any) -> ElementTree.ElementTree:
        """Download request as XML data type

        :param url: url to Sentinel Hub's services or other sources from where the data is downloaded
        :param kwargs: Any other parameters that are passed to DownloadRequest class
        :return: request response as XML instance
        """
        request = DownloadRequest(url=url, data_type=MimeType.XML, **kwargs)
        return self._single_download_decoded(request)

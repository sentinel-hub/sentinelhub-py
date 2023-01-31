"""
Module implementing error handlers which can occur during download procedure
"""
import functools
import logging
import sys
import time
from typing import Callable, Optional, TypeVar

import requests

from ..config import SHConfig
from ..decoding import decode_sentinelhub_err_msg
from ..exceptions import DownloadFailedException
from .models import DownloadRequest

if sys.version_info < (3, 8):
    from typing_extensions import Protocol
else:
    from typing import Protocol  # pylint: disable=ungrouped-imports


class _HasConfig(Protocol):
    """Interface of objects with a config."""

    config: SHConfig


Self = TypeVar("Self")
SelfWithConfig = TypeVar("SelfWithConfig", bound=_HasConfig)
T = TypeVar("T")


LOGGER = logging.getLogger(__name__)


def fail_user_errors(download_func: Callable[[Self, DownloadRequest], T]) -> Callable[[Self, DownloadRequest], T]:
    """Decorator function for handling user errors"""

    @functools.wraps(download_func)
    def new_download_func(self: Self, request: DownloadRequest) -> T:
        try:
            return download_func(self, request)
        except requests.HTTPError as exception:
            if (
                exception.response.status_code < requests.status_codes.codes.INTERNAL_SERVER_ERROR
                and exception.response.status_code != requests.status_codes.codes.TOO_MANY_REQUESTS
            ):
                raise DownloadFailedException(
                    _create_download_failed_message(exception, request.url), request_exception=exception
                ) from exception
            raise exception from exception

    return new_download_func


def retry_temporary_errors(
    download_func: Callable[[SelfWithConfig, DownloadRequest], T]
) -> Callable[[SelfWithConfig, DownloadRequest], T]:
    """Decorator function for handling server and connection errors"""
    backoff_coefficient = 3

    @functools.wraps(download_func)
    def new_download_func(self: SelfWithConfig, request: DownloadRequest) -> T:
        download_attempts = self.config.max_download_attempts
        sleep_time = self.config.download_sleep_time

        for attempt_idx in range(download_attempts):
            try:
                return download_func(self, request)

            except requests.RequestException as exception:
                attempts_left = download_attempts - (attempt_idx + 1)
                if not (
                    _is_temporary_problem(exception)
                    or (
                        isinstance(exception, requests.HTTPError)
                        and exception.response.status_code >= requests.status_codes.codes.INTERNAL_SERVER_ERROR
                    )
                ):
                    raise exception from exception

                if attempts_left <= 0:
                    message = _create_download_failed_message(exception, request.url)
                    raise DownloadFailedException(message, request_exception=exception) from exception

                LOGGER.debug(
                    "Download attempt failed: %s\n%d attempts left, will retry in %ds",
                    exception,
                    attempts_left,
                    sleep_time,
                )
                time.sleep(sleep_time)
                sleep_time *= backoff_coefficient

        raise DownloadFailedException(
            "No download attempts available - configuration parameter max_download_attempts should be greater than 0"
        )

    return new_download_func


def fail_missing_file(download_func: Callable[[Self, DownloadRequest], T]) -> Callable[[Self, DownloadRequest], T]:
    """A decorator for raising an error if a file is missing"""

    @functools.wraps(download_func)
    def new_download_func(self: Self, request: DownloadRequest) -> T:
        try:
            return download_func(self, request)
        except requests.HTTPError as exception:
            if exception.response.status_code == requests.status_codes.codes.NOT_FOUND:
                raise DownloadFailedException(
                    f"File in location {request.url} is missing", request_exception=exception
                ) from exception

            raise exception from exception

    return new_download_func


def _is_temporary_problem(exception: Exception) -> bool:
    """Checks if the obtained exception is temporary and if download attempt should be repeated

    :param exception: Exception raised during download
    :return: `True` if exception is temporary and `False` otherwise
    """
    return isinstance(exception, (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError))


def _create_download_failed_message(exception: Exception, url: Optional[str]) -> str:
    """Creates message describing why download has failed

    :param exception: Exception raised during download
    :param url: A URL from where download was attempted
    :return: Error message
    """
    message = f"Failed to download from:\n{url}\nwith {exception.__class__.__name__}:\n{exception}"

    if _is_temporary_problem(exception):
        if isinstance(exception, requests.ConnectionError):
            message += "\nPlease check your internet connection and try again."
        else:
            message += (
                "\nThere might be a problem in connection or the server failed to process "
                "your request. Please try again."
            )
    elif isinstance(exception, requests.HTTPError):
        server_message = decode_sentinelhub_err_msg(exception.response)
        message += f'\nServer response: "{server_message}"'

    return message

"""
Module defining custom package exceptions
"""
import warnings
from typing import Optional

import requests


class BaseSentinelHubException(Exception):
    """A base package exception"""


class DownloadFailedException(BaseSentinelHubException):
    """General exception which is raised whenever download fails"""

    def __init__(self, msg: str, *, request_exception: Optional[requests.RequestException] = None) -> None:
        super().__init__(msg)
        self.request_exception = request_exception


class OutOfRequestsException(DownloadFailedException):
    """An exception raised whenever download cannot be done because user has run out of requests or processing units"""


class AwsDownloadFailedException(DownloadFailedException):
    """This exception is raised when download fails because of a missing file in AWS"""


class ImageDecodingError(BaseSentinelHubException):
    """This exception is raised when downloaded image is not properly decoded"""


class MissingDataInRequestException(BaseSentinelHubException):
    """This exception is raised when an iteration is performed on a request without data."""


class HashedNameCollisionException(BaseSentinelHubException):
    """This exception is raised when two different requests are assigned the same hashed name."""


class SHDeprecationWarning(DeprecationWarning):
    """A custom deprecation warning for sentinelhub-py package"""


class SHUserWarning(UserWarning):
    """A custom user warning for sentinelhub-py package"""


class SHRuntimeWarning(RuntimeWarning):
    """A custom runtime warning for sentinelhub-py package"""


class SHRateLimitWarning(SHRuntimeWarning):
    """A custom runtime warning in case user hit the rate limit for downloads"""


warnings.simplefilter("default", SHDeprecationWarning)
warnings.simplefilter("default", SHUserWarning)
warnings.simplefilter("always", SHRuntimeWarning)
warnings.simplefilter("always", SHRateLimitWarning)

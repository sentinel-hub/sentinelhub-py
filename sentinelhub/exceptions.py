"""Module defining custom package exceptions."""
import functools
import warnings
from typing import Any, Callable, Optional, Type

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


def deprecated_function(
    category: Type[DeprecationWarning] = SHDeprecationWarning, message_suffix: Optional[str] = None
) -> Callable[[Callable], Callable]:
    """A parametrized function decorator, which signals that the function has been deprecated when called.

    Has to use paranthesis even when no custom parameters are used, e.g. `@deprecated_function()`.
    """

    def deco(func: Callable) -> Callable:
        message = f"Function `{func.__name__}` has been deprecated."
        if message_suffix:
            message += " " + message_suffix

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            warnings.warn(message, category=category, stacklevel=2)
            return func(*args, **kwargs)

        return wrapper

    return deco


def deprecated_class(
    category: Type[DeprecationWarning] = SHDeprecationWarning, message_suffix: Optional[str] = None
) -> Callable[[Type], Type]:
    """A parametrized class decorator, which signals that the class has been deprecated when initialized.

    Has to use paranthesis even when no custom parameters are used, e.g. `@deprecated_class()`.
    """

    def deco(class_object: Type) -> Type:
        message = f"Class `{class_object.__name__}` has been deprecated."
        if message_suffix:
            message += " " + message_suffix

        old_init = class_object.__init__

        def warn_and_init(self: Any, *args: Any, **kwargs: Any) -> None:
            warnings.warn(message, category=category, stacklevel=2)
            old_init(self, *args, **kwargs)

        class_object.__init__ = warn_and_init
        return class_object

    return deco

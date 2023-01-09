"""Deprecated module for types, moved to `sentinelhub.types`."""
from warnings import warn

from .exceptions import SHDeprecationWarning
from .types import *  # noqa # pylint: disable=wildcard-import,unused-wildcard-import

warn(
    "The module `sentinelhub.type_utils` is deprecated, use `sentinelhub.types` instead.",
    category=SHDeprecationWarning,
    stacklevel=2,
)

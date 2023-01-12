"""Module with custom types and utilities used in sentinelhub-py."""
import datetime as dt
import sys
from typing import Any, Dict, Tuple, Union

RawTimeType = Union[None, str, dt.date]
RawTimeIntervalType = Tuple[RawTimeType, RawTimeType]
JsonDict = Dict[str, Any]
Json = Union[JsonDict, list, str, float, int, None]


if sys.version_info < (3, 8):
    from typing_extensions import Literal  # pylint: disable=unused-import
else:
    from typing import Literal  # pylint: disable=ungrouped-imports  # noqa: F401

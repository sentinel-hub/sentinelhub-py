"""Module with custom types and utilities used in sentinelhub-py."""

import datetime as dt
from typing import Any, Dict, Tuple, Union

from typing_extensions import TypeAlias

RawTimeType: TypeAlias = Union[None, str, dt.date]
RawTimeIntervalType: TypeAlias = Tuple[RawTimeType, RawTimeType]
JsonDict: TypeAlias = Dict[str, Any]
Json: TypeAlias = Union[JsonDict, list, str, float, int, None]

"""
Module with custom types and utilities.
"""
import datetime as dt
from typing import Any, Dict, Tuple, Union

RawTimeType = Union[None, str, dt.date]
RawTimeIntervalType = Tuple[RawTimeType, RawTimeType]
JsonDict = Dict[str, Any]
Json = Union[JsonDict, list, str, float, int, None]

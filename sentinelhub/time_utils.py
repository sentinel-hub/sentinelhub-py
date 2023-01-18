"""
Module with useful time/date functions
"""
import datetime as dt
from typing import Any, Iterable, List, Optional, Tuple, TypeVar, Union, overload

import dateutil.parser
import dateutil.tz

from .types import Literal, RawTimeIntervalType, RawTimeType

TimeType = TypeVar("TimeType", dt.date, dt.datetime)  # pylint: disable=invalid-name


def is_valid_time(time: str) -> bool:
    """Check if input string represents a valid time/date stamp

    :param time: A string containing a time/date.
    :return: `True` is string is a valid time/date, `False` otherwise.
    """
    try:
        dateutil.parser.parse(time)
        return True
    except dateutil.parser.ParserError:
        return False


@overload
def parse_time(
    time_input: RawTimeType,
    *,
    force_datetime: Literal[False] = False,
    allow_undefined: Literal[False] = False,
    **kwargs: Any
) -> dt.date:
    ...


@overload
def parse_time(
    time_input: RawTimeType, *, force_datetime: Literal[True], allow_undefined: Literal[False] = False, **kwargs: Any
) -> dt.datetime:
    ...


@overload
def parse_time(
    time_input: RawTimeType, *, force_datetime: Literal[False] = False, allow_undefined: bool = False, **kwargs: Any
) -> Optional[dt.date]:
    ...


@overload
def parse_time(
    time_input: RawTimeType, *, force_datetime: Literal[True], allow_undefined: bool = False, **kwargs: Any
) -> Optional[dt.datetime]:
    ...


def parse_time(
    time_input: RawTimeType, *, force_datetime: bool = False, allow_undefined: bool = False, **kwargs: Any
) -> Optional[dt.date]:
    """Parse input time/date string

    :param time_input: An input representation of a time.
    :param force_datetime: If True it will always return datetime.datetime object, if False it can also return only
        `datetime.date` object if only date is provided as input.
    :param allow_undefined: Flag to allow parsing None or '..' into None.
    :param kwargs: Keyword arguments to be passed to `dateutil.parser.parse`. Example: `ignoretz=True`.
    :return: A parsed datetime representing the time.
    """

    if time_input is None or time_input == "..":
        if allow_undefined:
            return None
        raise ValueError("Input is undefined but `allow_undefined` is set to `False`.")

    if isinstance(time_input, dt.date):
        if force_datetime and not isinstance(time_input, dt.datetime):
            return date_to_datetime(time_input)

        if kwargs.get("ignoretz") and isinstance(time_input, dt.datetime):
            return time_input.replace(tzinfo=None)

        return time_input

    time = dateutil.parser.parse(time_input, **kwargs)
    if force_datetime or len(time_input) > 10:  # This check is not very accurate, but it works for ISO format
        return time
    return time.date()


def parse_time_interval(
    time: Union[RawTimeType, RawTimeIntervalType], allow_undefined: bool = False, **kwargs: Any
) -> Tuple[Optional[dt.datetime], Optional[dt.datetime]]:
    """Parse input into an interval of two times, specifying start and end time, into datetime objects.

    The input time can have the following formats, which will be parsed as:

    * `YYYY-MM-DD` -> `[YYYY-MM-DD:T00:00:00, YYYY-MM-DD:T23:59:59]`
    * `YYYY-MM-DDThh:mm:ss` -> `[YYYY-MM-DDThh:mm:ss, YYYY-MM-DDThh:mm:ss]`
    * list or tuple of two dates in form `YYYY-MM-DD` -> `[YYYY-MM-DDT00:00:00, YYYY-MM-DDT23:59:59]`
    * list or tuple of two dates in form `YYYY-MM-DDThh:mm:ss` -> `[YYYY-MM-DDThh:mm:ss, YYYY-MM-DDThh:mm:ss]`

    All input times can also be specified as `datetime` objects. Instances of `datetime.date` will be treated as
    `YYYY-MM-DD` and instance of `datetime.datetime` will be treated as `YYYY-MM-DDThh:mm:ss`.

    :param time: An input representation of a time interval.
    :param allow_undefined: Boolean flag controls if None or '..' are allowed.
    :param kwargs: Keyword arguments to be passed to `parse_time` function.
    :return: A pair of datetime objects defining the time interval.
    :raises: ValueError
    """
    date_interval: Tuple[Optional[dt.date], Optional[dt.date]]

    if allow_undefined and time in [None, ".."]:
        date_interval = None, None
    elif isinstance(time, (str, dt.date)):
        parsed_time = parse_time(time, **kwargs)
        date_interval = parsed_time, parsed_time
    elif isinstance(time, (tuple, list)) and len(time) == 2:
        start_date = parse_time(time[0], allow_undefined=allow_undefined, **kwargs)
        end_date = parse_time(time[1], allow_undefined=allow_undefined, **kwargs)
        date_interval = start_date, end_date
    else:
        raise ValueError("Time must be a string/datetime object or tuple/list of 2 strings/datetime objects")

    start_time, end_time = date_interval
    if not isinstance(start_time, dt.datetime) and start_time is not None:
        start_time = date_to_datetime(start_time)
    if not isinstance(end_time, dt.datetime) and end_time is not None:
        end_time = date_to_datetime(end_time, dt.time(hour=23, minute=59, second=59))

    if start_time and end_time and start_time > end_time:
        raise ValueError("Start of time interval is larger than end of time interval")

    return start_time, end_time


@overload
def serialize_time(timestamp_input: Optional[dt.date], *, use_tz: bool = False) -> str:
    ...


@overload
def serialize_time(timestamp_input: Iterable[Optional[dt.date]], *, use_tz: bool = False) -> Tuple[str, ...]:
    ...


def serialize_time(
    timestamp_input: Union[None, dt.date, Iterable[Optional[dt.date]]], *, use_tz: bool = False
) -> Union[str, Tuple[str, ...]]:
    """Transforms datetime objects into ISO 8601 strings.

    :param timestamp_input: A datetime object or a tuple of datetime objects.
    :param use_tz: If `True` it will ensure that the serialized string contains a timezone information (typically
        with `Z` at the end instead of +00:00). If `False` it will make sure to remove any timezone information.
    :return: Timestamp(s) serialized into string(s).
    """
    if isinstance(timestamp_input, Iterable):
        return tuple(serialize_time(timestamp, use_tz=use_tz) for timestamp in timestamp_input)

    if timestamp_input is None:
        return ".."

    if not isinstance(timestamp_input, dt.date):
        raise ValueError("Expected a datetime object or a tuple of datetime objects")

    if use_tz:
        if not isinstance(timestamp_input, dt.datetime):
            raise ValueError(
                "Cannot ensure timezone information for datetime.date objects, use datetime.datetime instead"
            )

        if not timestamp_input.tzinfo:
            timestamp_input = timestamp_input.replace(tzinfo=dateutil.tz.tzutc())

    elif isinstance(timestamp_input, dt.datetime) and timestamp_input.tzinfo:
        timestamp_input = timestamp_input.replace(tzinfo=None)

    return timestamp_input.isoformat().replace("+00:00", "Z")


def date_to_datetime(date: dt.date, time: Optional[dt.time] = None) -> dt.datetime:
    """Converts a date object into datetime object.

    :param date: A date object.
    :param time: An option time object, if not provided it will replace it with `00:00:00`.
    :return: A datetime object derived from date and time.
    """
    if time is None:
        time = dt.datetime.min.time()
    return dt.datetime.combine(date, time)


def filter_times(timestamps: Iterable[TimeType], time_difference: dt.timedelta) -> List[TimeType]:
    """Filters out timestamps within time_difference, preserving only the oldest timestamp.

    :param timestamps: A list of timestamps.
    :param time_difference: A time difference threshold.
    :return: An ordered list of timestamps `d_1 <= d_2 <= ... <= d_n` such that `d_(i+1)-d_i > time_difference`.
    """
    timestamps = sorted(set(timestamps))

    filtered_timestamps: List[TimeType] = []
    for current_timestamp in timestamps:
        if not filtered_timestamps or current_timestamp - filtered_timestamps[-1] > time_difference:
            filtered_timestamps.append(current_timestamp)

    return filtered_timestamps

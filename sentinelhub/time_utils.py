"""
Module with useful time/date functions
"""
import datetime as dt

import dateutil.parser
import dateutil.tz


def is_valid_time(time):
    """ Check if input string represents a valid time/date stamp

    :param time: a string containing a time/date stamp
    :type time: str
    :return: `True` is string is a valid time/date stamp, `False` otherwise
    :rtype: bool
    """
    try:
        dateutil.parser.parse(time)
        return True
    except dateutil.parser.ParserError:
        return False


def parse_time(time_input, *, force_datetime=False, allow_undefined=False, **kwargs):
    """ Parse input time/date string

    :param time_input: time/date to parse
    :type time_input: str or datetime.date or datetime.datetime
    :param force_datetime: If True it will always return datetime.datetime object, if False it can also return only
        datetime.date object if only date is provided as input.
    :type force_datetime: bool
    :param allow_undefined: Flag to allow parsing None or '..' into None
    :param allow_undefined: bool (default is False)
    :param kwargs: Any keyword arguments to be passed to `dateutil.parser.parse`. Example: `ignoretz=True`
    :return: A datetime object
    :rtype: datetime.datetime or datetime.date
    """

    if allow_undefined and time_input in [None, '..']:
        return None

    if isinstance(time_input, dt.date):
        if force_datetime and not isinstance(time_input, dt.datetime):
            return date_to_datetime(time_input)

        if kwargs.get('ignoretz') and isinstance(time_input, dt.datetime):
            return time_input.replace(tzinfo=None)

        return time_input

    time = dateutil.parser.parse(time_input, **kwargs)
    if force_datetime or len(time_input) > 10:  # This check is not very accurate but it works for iso format
        return time
    return time.date()


def parse_time_interval(time, allow_undefined=False, **kwargs):
    """ Parse input into an interval of two times, specifying start and end time, into datetime objects.

    The input time can have the following formats, which will be parsed as:

    * `YYYY-MM-DD` -> `[YYYY-MM-DD:T00:00:00, YYYY-MM-DD:T23:59:59]`
    * `YYYY-MM-DDThh:mm:ss` -> `[YYYY-MM-DDThh:mm:ss, YYYY-MM-DDThh:mm:ss]`
    * list or tuple of two dates in form `YYYY-MM-DD` -> `[YYYY-MM-DDT00:00:00, YYYY-MM-DDT23:59:59]`
    * list or tuple of two dates in form `YYYY-MM-DDThh:mm:ss` -> `[YYYY-MM-DDThh:mm:ss, YYYY-MM-DDThh:mm:ss]`

    All input times can also be specified as `datetime` objects. Instances of `datetime.date` will be treated as
    `YYYY-MM-DD` and instance of `datetime.datetime` will be treated as `YYYY-MM-DDThh:mm:ss`.

    :param allow_undefined: Boolean flag controls if None or '..' are allowed
    :param allow_undefined: bool
    :param time: An input time
    :type time: str or datetime.datetime or (str, str) or (datetime.datetime, datetime.datetime)
    :return: interval of start and end date of the form `YYYY-MM-DDThh:mm:ss`
    :rtype: (datetime.datetime, datetime.datetime)
    :raises: ValueError
    """
    if allow_undefined and time in [None, '..']:
        date_interval = None, None
    elif isinstance(time, (str, dt.date)):
        parsed_time = parse_time(time, **kwargs)
        date_interval = parsed_time, parsed_time
    elif isinstance(time, (tuple, list)) and len(time) == 2:
        date_interval = parse_time(time[0], allow_undefined=allow_undefined, **kwargs), \
                        parse_time(time[1], allow_undefined=allow_undefined, **kwargs)
    else:
        raise ValueError('Time must be a string/datetime object or tuple/list of 2 strings/datetime objects')

    start_time, end_time = date_interval
    if not isinstance(start_time, dt.datetime) and start_time is not None:
        start_time = date_to_datetime(start_time)
    if not isinstance(end_time, dt.datetime) and end_time is not None:
        end_time = date_to_datetime(end_time, dt.time(hour=23, minute=59, second=59))

    if start_time and end_time and start_time > end_time:
        raise ValueError('Start of time interval is larger than end of time interval')

    return start_time, end_time


def serialize_time(timestamp_input, *, use_tz=False):
    """ Transforms datetime objects into ISO 8601 strings

    :param timestamp_input: A datetime object or a tuple of datetime objects
    :type timestamp_input: datetime.date or datetime.datetime or tuple(datetime.date or datetime.datetime)
    :param use_tz: If `True` it will ensure that the serialized string contains a timezone information (typically
        with `Z` at the end instead of +00:00). If `False` it will make sure to remove any timezone information
    :type use_tz: bool
    :return: Timestamp(s) serialized into string(s)
    :rtype: str or tuple(str)
    """
    if isinstance(timestamp_input, tuple):
        return tuple(serialize_time(timestamp, use_tz=use_tz) for timestamp in timestamp_input)

    if timestamp_input is None:
        return '..'

    if not isinstance(timestamp_input, dt.date):
        raise ValueError('Expected a datetime object or a tuple of datetime objects')

    if use_tz and not isinstance(timestamp_input, dt.datetime):
        raise ValueError('Cannot ensure timezone information for datetime.date objects, use datetime.datetime instead')

    if use_tz and not timestamp_input.tzinfo:
        timestamp_input = timestamp_input.replace(tzinfo=dateutil.tz.tzutc())

    if not use_tz and isinstance(timestamp_input, dt.datetime) and timestamp_input.tzinfo:
        timestamp_input = timestamp_input.replace(tzinfo=None)

    return timestamp_input.isoformat().replace('+00:00', 'Z')


def date_to_datetime(date, time=None):
    """ Converts a date object into datetime object

    :param date: a date object
    :type date: datetime.date
    :param time: an option time object, if not provided it will replace it with 00:00:00
    :type time: datetime.time
    :return: A datetime object derived from date and time
    :rtype: datetime.datetime
    """
    if time is None:
        time = dt.datetime.min.time()
    return dt.datetime.combine(date, time)


def filter_times(timestamps, time_difference):
    """ Filters out timestamps within time_difference, preserving only the oldest timestamp.

    :param timestamps: A list of timestamps
    :type timestamps: list(datetime.datetime)
    :param time_difference: A time difference threshold
    :type time_difference: datetime.timedelta
    :return: An ordered list of timestamps `d_1<=d_2<=...<=d_n` such that `d_(i+1)-d_i > time_difference`
    :rtype: list(datetime.datetime)
    """
    timestamps = sorted(set(timestamps))

    filtered_timestamps = []
    for current_timestamp in timestamps:
        if not filtered_timestamps or current_timestamp - filtered_timestamps[-1] > time_difference:
            filtered_timestamps.append(current_timestamp)

    return filtered_timestamps

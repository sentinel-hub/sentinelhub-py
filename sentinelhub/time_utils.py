"""
Module with useful time/date functions
"""

import datetime
import dateutil.parser

from .constants import SHConstants


def get_dates_in_range(start_date, end_date):
    """ Get all dates within input start and end date in ISO 8601 format

    :param start_date: start date in ISO 8601 format
    :type start_date: str
    :param end_date: end date in ISO 8601 format
    :type end_date: str
    :return: list of dates between start_date and end_date in ISO 8601 format
    :rtype: list of str
    """
    start_dt = iso_to_datetime(start_date)
    end_dt = iso_to_datetime(end_date)
    num_days = int((end_dt - start_dt).days)
    return [datetime_to_iso(start_dt + datetime.timedelta(i)) for i in range(num_days + 1)]


def next_date(date):
    """ Get date of day after input date in ISO 8601 format

    For instance, if input date is ``'2017-03-12'``, the function returns ``'2017-03-13'``

    :param date: input date in ISO 8601 format
    :type date: str
    :return: date after input date in ISO 8601 format
    :rtype: str
    """
    dtm = iso_to_datetime(date)
    return datetime_to_iso(dtm + datetime.timedelta(1))


def prev_date(date):
    """ Get date of day previous to input date in ISO 8601 format

    For instance, if input date is ``'2017-03-12'``, the function returns ``'2017-03-11'``

    :param date: input date in ISO 8601 format
    :type date: str
    :return: date previous to input date in ISO 8601 format
    :rtype: str
    """
    dtm = iso_to_datetime(date)
    return datetime_to_iso(dtm - datetime.timedelta(1))


def iso_to_datetime(date):
    """ Convert ISO 8601 time format to datetime format

    This function converts a date in ISO format, e.g. ``2017-09-14`` to a `datetime` instance, e.g.
    ``datetime.datetime(2017,9,14,0,0)``

    :param date: date in ISO 8601 format
    :type date: str
    :return: datetime instance
    :rtype: datetime
    """
    chunks = list(map(int, date.split('T')[0].split('-')))
    return datetime.datetime(chunks[0], chunks[1], chunks[2])


def datetime_to_iso(date, only_date=True):
    """ Convert datetime format to ISO 8601 time format

    This function converts a date in datetime instance, e.g. ``datetime.datetime(2017,9,14,0,0)`` to ISO format,
    e.g. ``2017-09-14``

    :param date: datetime instance to convert
    :type date: datetime
    :param only_date: whether to return date only or also time information. Default is `True`
    :type only_date: bool
    :return: date in ISO 8601 format
    :rtype: str
    """
    if only_date:
        return date.isoformat().split('T')[0]
    return date.isoformat()


def get_current_date():
    """ Get current date in ISO 8601 format

    :return: current date in ISO 8601 format
    :rtype: str
    """
    return datetime_to_iso(datetime.datetime.now())


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
    except BaseException:
        return False


def parse_time(time_input):
    """ Parse input time/date string into ISO 8601 string

    :param time_input: time/date to parse
    :type time_input: str or datetime.date or datetime.datetime
    :return: parsed string in ISO 8601 format
    :rtype: str
    """
    if isinstance(time_input, datetime.date):
        return time_input.isoformat()  # datetime.date only returns date, datetime.datetime also returns time

    if len(time_input) < 8:
        raise ValueError('Invalid time string {}.\n'
                         'Please specify time in formats YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS'.format(time_input))
    time = dateutil.parser.parse(time_input)
    if len(time_input) <= 10:
        return time.date().isoformat()
    return time.isoformat()


def parse_time_interval(time):
    """ Parse input into an interval of two times, specifying start and end time, in ISO 8601 format, for example:

    ``(2017-01-15:T00:00:00, 2017-01-16:T23:59:59)``

    The input time can have the following formats, which will be parsed as:

    * `YYYY-MM-DD` -> `[YYYY-MM-DD:T00:00:00, YYYY-MM-DD:T23:59:59]`
    * `YYYY-MM-DDThh:mm:ss` -> `[YYYY-MM-DDThh:mm:ss, YYYY-MM-DDThh:mm:ss]`
    * list or tuple of two dates in form `YYYY-MM-DD` -> `[YYYY-MM-DDT00:00:00, YYYY-MM-DDT23:59:59]`
    * list or tuple of two dates in form `YYYY-MM-DDThh:mm:ss` -> `[YYYY-MM-DDThh:mm:ss, YYYY-MM-DDThh:mm:ss]`

    All input times can also be specified as `datetime` objects. Instances of `datetime.date` will be treated as
    `YYYY-MM-DD` and instance of `datetime.datetime` will be treated as `YYYY-MM-DDThh:mm:ss`.

    :param time: An input time
    :type time: str or datetime.datetime
    :return: interval of start and end date of the form `YYYY-MM-DDThh:mm:ss`
    :rtype: (str, str)
    :raises: ValueError
    """
    if time is SHConstants.LATEST:
        date_interval = '1985-01-01', get_current_date()
    elif isinstance(time, (str, datetime.date)):
        parsed_time = parse_time(time)
        date_interval = parsed_time, parsed_time
    elif isinstance(time, (tuple, list)) and len(time) == 2:
        date_interval = parse_time(time[0]), parse_time(time[1])
    else:
        raise ValueError('Time must be a string/datetime object or tuple/list of 2 strings/datetime objects')

    if 'T' not in date_interval[0]:
        date_interval = date_interval[0] + 'T00:00:00', date_interval[1]
    if 'T' not in date_interval[1]:
        date_interval = date_interval[0], date_interval[1] + 'T23:59:59'

    if date_interval[1] < date_interval[0]:
        raise ValueError('Start of time interval is larger than end of time interval')

    return date_interval

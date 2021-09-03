"""
Unit tests for time utility functions
"""
import datetime as dt

import pytest
import dateutil.tz

from sentinelhub import time_utils


TEST_DATE = dt.date(year=2015, month=4, day=12)
TEST_DATETIME = dt.datetime(year=2015, month=4, day=12, hour=12, minute=32, second=14)
TEST_DATETIME_TZ = dt.datetime(year=2015, month=4, day=12, hour=12, minute=32, second=14, tzinfo=dateutil.tz.tzutc())
TEST_TIME_START = dt.datetime(year=2015, month=4, day=12)
TEST_TIME_END = dt.datetime(year=2015, month=4, day=12, hour=23, minute=59, second=59)

TIMES = [
    dt.datetime(year=2005, month=12, day=16, hour=2),
    dt.datetime(year=2005, month=12, day=16, hour=10),
    dt.datetime(year=2005, month=12, day=16, hour=23),
    dt.datetime(year=2005, month=12, day=17, hour=2),
    dt.datetime(year=2005, month=12, day=18, hour=2)
]

DELTAS = [
    dt.timedelta(0),
    dt.timedelta(hours=5),
    dt.timedelta(hours=12),
    dt.timedelta(days=1),
]


@pytest.mark.parametrize('time_input,is_valid', [
    ('2017-01-32', False),
    ('2017-13-1', False),
    ('2017-02-29', False),
    ('2020-02-29', True),
    ('2020-02-30', False)
])
def test_is_valid_time(time_input, is_valid):
    assert time_utils.is_valid_time(time_input) is is_valid


@pytest.mark.parametrize('time_input,params,expected_output', [
    ('2015.4.12', {}, TEST_DATE),
    ('2015.4.12', {'force_datetime': True}, dt.datetime(year=2015, month=4, day=12)),
    ('2015.4.12T12:32:14', {}, TEST_DATETIME),
    ('2015.4.12T12:32:14Z', {}, TEST_DATETIME_TZ),
    ('2015.4.12T12:32:14Z', {'ignoretz': True}, TEST_DATETIME),
    ('..', {'allow_undefined': True}, None),
    (None, {'allow_undefined': True}, None),
    (TEST_DATETIME, {}, TEST_DATETIME),
    (TEST_DATETIME_TZ, {}, TEST_DATETIME_TZ),
    (TEST_DATETIME_TZ, {'ignoretz': True}, TEST_DATETIME),
    (TEST_DATE, {}, TEST_DATE),
    (TEST_DATE, {'force_datetime': True}, dt.datetime(year=2015, month=4, day=12))
])
def test_parse_time(time_input, params, expected_output):
    parsed_time = time_utils.parse_time(time_input, **params)
    assert parsed_time == expected_output


@pytest.mark.parametrize('time_input,params,expected_output', [
    ('2015.4.12', {}, (TEST_TIME_START, TEST_TIME_END)),
    ('2015.4.12T12:32:14', {}, (TEST_DATETIME, TEST_DATETIME)),
    ('2015.4.12T12:32:14Z', {}, (TEST_DATETIME_TZ, TEST_DATETIME_TZ)),
    ('2015.4.12T12:32:14Z', {'ignoretz': True}, (TEST_DATETIME, TEST_DATETIME)),
    (('2015-4-12', '2017-4-12'), {}, (TEST_TIME_START, TEST_TIME_END.replace(year=2017))),
    (('2015.4.12T12:32:14', '2017.4.12T12:32:14'), {}, (TEST_DATETIME, TEST_DATETIME.replace(year=2017))),
    ((TEST_DATE, TEST_DATE.replace(year=2017)), {}, (TEST_TIME_START, TEST_TIME_END.replace(year=2017))),
    ((TEST_DATETIME, TEST_DATETIME.replace(year=2017)), {}, (TEST_DATETIME, TEST_DATETIME.replace(year=2017))),
    (None, {'allow_undefined': True}, (None, None)),
    ((None, None), {'allow_undefined': True}, (None, None)),
    ((TEST_DATETIME, None), {'allow_undefined': True}, (TEST_DATETIME, None)),
    ((None, TEST_DATE), {'allow_undefined': True}, (None, TEST_TIME_END)),
])
def test_parse_time_interval(time_input, params, expected_output):
    parsed_interval = time_utils.parse_time_interval(time_input, **params)
    assert parsed_interval == expected_output


@pytest.mark.parametrize('time_input,params,expected_output', [
    (None, {}, '..'),
    (TEST_DATE, {}, '2015-04-12'),
    (TEST_DATETIME, {}, '2015-04-12T12:32:14'),
    (TEST_DATETIME, {'use_tz': True}, '2015-04-12T12:32:14Z'),
    (TEST_DATETIME_TZ, {'use_tz': False}, '2015-04-12T12:32:14'),
    (TEST_DATETIME_TZ, {'use_tz': True}, '2015-04-12T12:32:14Z'),
    ((TEST_DATE, TEST_DATETIME, TEST_DATETIME_TZ), {}, ('2015-04-12', '2015-04-12T12:32:14', '2015-04-12T12:32:14')),
    ((None, None), {}, ('..', '..')),
    ((TEST_DATETIME, None), {}, ('2015-04-12T12:32:14', '..')),
    ((None, TEST_DATETIME), {}, ('..', '2015-04-12T12:32:14'))
])
def test_serialize_time(time_input, params, expected_output):
    serialized_result = time_utils.serialize_time(time_input, **params)
    assert serialized_result == expected_output


@pytest.mark.parametrize('input_date,input_time,expected_output', [
    (TEST_DATE, None, TEST_TIME_START),
    (TEST_DATE, dt.time(hour=12, minute=32, second=14), TEST_DATETIME)
])
def test_date_to_datetime(input_date, input_time, expected_output):
    result_datetime = time_utils.date_to_datetime(input_date, time=input_time)
    assert result_datetime == expected_output


@pytest.mark.parametrize('input_timestamps,time_difference,expected_result', [
    ([TIMES[0]], DELTAS[1], [TIMES[0]]),
    ([TIMES[0], TIMES[1]], DELTAS[0], [TIMES[0], TIMES[1]]),
    ([TIMES[2], TIMES[0], TIMES[1], TIMES[1], TIMES[0]], DELTAS[2], [TIMES[0], TIMES[2]]),
    ([TIMES[0], TIMES[3]], DELTAS[3], [TIMES[0]]),
    ([TIMES[3], TIMES[4]], DELTAS[3], [TIMES[3]]),
    ([TIMES[0], TIMES[3], TIMES[4]], DELTAS[3], [TIMES[0], TIMES[4]])
])
def test_filter_times(input_timestamps, time_difference, expected_result):
    result = time_utils.filter_times(input_timestamps, time_difference)
    assert result == expected_result

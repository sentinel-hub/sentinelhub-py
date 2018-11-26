import unittest
import datetime

from sentinelhub import time_utils, TestSentinelHub


class TestTime(TestSentinelHub):
    def test_get_dates_in_range(self):
        test_pairs = [
            (('2018-01-01', '2017-12-31'), 0),
            (('2017-01-01', '2017-01-31'), 31),
            (('2017-02-01', '2017-03-01'), 28+1),
            (('2018-02-01', '2018-03-01'), 28+1),
            (('2020-02-01', '2020-03-01'), 29+1),
            (('2018-01-01', '2018-12-31'), 365),
            (('2020-01-01', '2020-12-31'), 366)
        ]
        for daterange, nr_dates in test_pairs:
            with self.subTest(msg=daterange):
                start_date, end_date = daterange
                dates = time_utils.get_dates_in_range(start_date, end_date)
                self.assertEqual(len(dates), nr_dates,
                                 msg='Expected {} dates, got {}'.format(str(len(dates)), str(nr_dates)))

    def test_next_date(self):
        test_pairs = [
            ('2017-12-31', '2018-01-01'),
            ('2018-02-28', '2018-03-01'),
            ('2020-02-28', '2020-02-29'),
            ('2020-02-29', '2020-03-01'),
            ('2018-01-05', '2018-01-06')
        ]
        for curr_date, next_date in test_pairs:
            with self.subTest(msg='{}/{}'.format(curr_date, next_date)):
                res_date = time_utils.next_date(curr_date)
                self.assertEqual(res_date, next_date,
                                 msg='Expected {}, got {}'.format(curr_date, next_date))

    def test_prev_date(self):
        test_pairs = [
            ('2018-02-28', '2018-03-01'),
            ('2017-12-31', '2018-01-01'),
            ('2018-01-31', '2018-02-01')
        ]
        for prev_date, curr_date in test_pairs:
            with self.subTest(msg='{}/{}'.format(prev_date, curr_date)):
                res_date = time_utils.prev_date(curr_date)
                self.assertEqual(prev_date, res_date,
                                 msg='Expected {}, got {}'.format(prev_date, res_date))

    def test_iso_to_datetime(self):
        test_pairs = [
            ('2018-01-01', datetime.datetime(2018, 1, 1)),
            ('2017-02-28', datetime.datetime(2017, 2, 28))
        ]
        for date_str, date_dt in test_pairs:
            with self.subTest(msg=date_str):
                res_dt = time_utils.iso_to_datetime(date_str)
                self.assertEqual(res_dt, date_dt,
                                 msg='Expected {}, got {}'.format(date_dt, res_dt))

    def test_datetime_to_iso(self):
        test_pairs = [
            (datetime.datetime(2018, 1, 1), '2018-01-01'),
            (datetime.datetime(2017, 2, 28), '2017-02-28')
        ]
        for date_dt, date_str in test_pairs:
            with self.subTest(msg=date_str):
                res_str = time_utils.datetime_to_iso(date_dt)
                self.assertEqual(res_str, date_str,
                                 msg='Expected {}, got {}'.format(date_str, res_str))

    def test_get_current_date(self):
        current_date = time_utils.get_current_date()
        self.assertTrue(isinstance(current_date, str), 'Expected date in str format')
        self.assertEqual(len(current_date), 10, 'Expected date length 10, got {}'.format(current_date))

    def test_is_valid_time(self):
        test_pairs = [
            ('2017-01-32', False),
            ('2017-13-1', False),
            ('2017-02-29', False),
            ('2020-02-29', True),
            ('2020-02-30', False)
        ]
        for iso_str, is_ok in test_pairs:
            with self.subTest(msg=iso_str):
                self.assertEqual(time_utils.is_valid_time(iso_str), is_ok,
                                 msg='Expected {}, got {}'.format(not is_ok, is_ok))

    def test_parse_time(self):
        test_pairs = [
            ('2015.4.12', '2015-04-12'),
            ('2015.4.12T12:32:14', '2015-04-12T12:32:14'),
            (datetime.date(year=2015, month=2, day=3), '2015-02-03'),
            (datetime.datetime(year=2015, month=2, day=3), '2015-02-03T00:00:00')
        ]

        for idx, (input_time, exp_time) in enumerate(test_pairs):
            with self.subTest(msg='Test case {}'.format(idx + 1)):
                parsed_time = time_utils.parse_time(input_time)
                self.assertEqual(parsed_time, exp_time, 'Expected {}, got {}'.format(exp_time, parsed_time))

    def test_parse_time_interval(self):
        test_pairs = [
            ('2015.4.12', ('2015-04-12T00:00:00', '2015-04-12T23:59:59')),
            ('2015-4-12T5:4:3', ('2015-04-12T05:04:03', '2015-04-12T05:04:03')),
            (('2015-4-12', '2017-4-12'), ('2015-04-12T00:00:00', '2017-04-12T23:59:59')),
            (('2015-4-12T5:4:3', '2017-4-12T5:4:3'), ('2015-04-12T05:04:03', '2017-04-12T05:04:03')),
            (datetime.date(year=2015, month=2, day=3), ('2015-02-03T00:00:00', '2015-02-03T23:59:59')),
            ((datetime.date(year=2015, month=2, day=3), datetime.date(year=2015, month=2, day=15)),
             ('2015-02-03T00:00:00', '2015-02-15T23:59:59')),
        ]

        for idx, (input_time, exp_interval) in enumerate(test_pairs):
            with self.subTest(msg='Test case {}'.format(idx + 1)):
                parsed_interval = time_utils.parse_time_interval(input_time)
                self.assertEqual(parsed_interval, exp_interval,
                                 'Expected {}, got {}'.format(exp_interval, parsed_interval))


if __name__ == '__main__':
    unittest.main()

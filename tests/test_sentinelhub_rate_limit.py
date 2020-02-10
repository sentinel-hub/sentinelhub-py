"""
Tests for utilities that implement rate-limiting in the package
"""
import unittest

from sentinelhub import TestCaseContainer
from sentinelhub.sentinelhub_rate_limit import SentinelHubRateLimit, PolicyBucket


class TestRateLimit(unittest.TestCase):
    pass


class TestPolicyBucket(unittest.TestCase):
    """ A class that tests PolicyBucket class
    """
    @classmethod
    def setUp(cls):
        cls.test_cases = [
            TestCaseContainer('Requests bucket',
                              PolicyBucket('REQUESTS', {"capacity": 30000,
                                                        "samplingPeriod": "PT744H",
                                                        "nanosBetweenRefills": 89280000000,
                                                        "niceSamplindPeriod": "31 days"}),
                              is_request_bucket=True, is_fixed=False,
                              elapsed_time=10, new_content=20000, cost_per_second=1000.0112007,
                              requests_completed=3000, wait_time=2679327.44),
            TestCaseContainer('Processing units bucket',
                              PolicyBucket('PROCESSING_UNITS', {"capacity": 300,
                                                                "samplingPeriod": "PT1M",
                                                                "nanosBetweenRefills": 200000000,
                                                                "niceSamplindPeriod": "1 minute"}),
                              is_request_bucket=False, is_fixed=False,
                              elapsed_time=10, new_content=200, cost_per_second=15.0,
                              requests_completed=50, wait_time=132.1),
            TestCaseContainer('Fixed units bucket',
                              PolicyBucket('REQUESTS', {"capacity": 10,
                                                        "samplingPeriod": "PT0S",
                                                        "nanosBetweenRefills": 9223372036854775807,
                                                        "niceSamplindPeriod": "Fixed amount of tokens"}),
                              is_request_bucket=True, is_fixed=True,
                              elapsed_time=10, new_content=5, cost_per_second=0.5,
                              requests_completed=10, wait_time=-1),
        ]

    def test_basic_bucket_methods(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                bucket = test_case.request

                self.assertEqual(bucket.is_request_bucket(), test_case.is_request_bucket)
                self.assertEqual(bucket.is_fixed(), test_case.is_fixed)
                self.assertTrue(repr(bucket).startswith('PolicyBucket('))

                original_content = bucket.content
                bucket.content = 0
                self.assertEqual(bucket.content, 0)
                bucket.content = original_content
                self.assertEqual(bucket.content, original_content)

    def test_cost_per_second(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                bucket = test_case.request

                self.assertAlmostEqual(bucket.count_cost_per_second(test_case.elapsed_time, test_case.new_content),
                                       test_case.cost_per_second, delta=1e-6)

    def test_expected_wait_time(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                bucket = test_case.request

                wait_time = bucket.get_expected_wait_time(
                    test_case.elapsed_time,
                    expected_process_num=2,
                    expected_cost_per_request=10,
                    requests_completed=test_case.requests_completed
                )

                self.assertAlmostEqual(wait_time, test_case.wait_time, delta=1e-6)


if __name__ == '__main__':
    unittest.main()

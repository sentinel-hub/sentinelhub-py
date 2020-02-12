"""
Tests for utilities that implement rate-limiting in the package
"""
import unittest
import copy
import time

from sentinelhub import TestCaseContainer
from sentinelhub.sentinelhub_rate_limit import SentinelHubRateLimit, PolicyBucket, PolicyType


class DummyService:

    def __init__(self, policy_buckets, units_per_request, process_time):

        self.policy_buckets = policy_buckets
        self.units_per_request = units_per_request
        self.process_time = process_time

        self.time = time.monotonic()

    def make_request(self):
        new_time = time.monotonic()
        time.sleep(self.process_time)
        elapsed_time = new_time - self.time

        for bucket in self.policy_buckets:
            bucket.content = min(bucket.content + elapsed_time * bucket.refill_per_second, bucket.capacity)

        self.time = new_time

        new_content_list = self._get_new_bucket_content()
        is_rate_limited = min(new_content_list) < 0

        if not is_rate_limited:
            for bucket, new_content in zip(self.policy_buckets, new_content_list):
                bucket.content = new_content

        return self._get_headers(is_rate_limited)

    def _get_new_bucket_content(self):
        costs = ((1 if bucket.is_request_bucket() else self.units_per_request)
                 for bucket in self.policy_buckets)

        return [bucket.content - cost for bucket, cost in zip(self.policy_buckets, costs)]

    def _get_headers(self, is_rate_limited):
        headers = {
            SentinelHubRateLimit.REQUEST_COUNT_HEADER: min(bucket.content for bucket in self.policy_buckets
                                                           if bucket.is_request_bucket()),
            SentinelHubRateLimit.UNITS_COUNT_HEADER: min(bucket.content for bucket in self.policy_buckets
                                                         if not bucket.is_request_bucket())
        }
        if is_rate_limited:
            headers[SentinelHubRateLimit.VIOLATION_HEADER] = True

            expected_request_wait_time = max(
                bucket.get_expected_wait_time(0, 1, 1, 0, 0) for bucket in self.policy_buckets
                if bucket.is_request_bucket()
            )
            if expected_request_wait_time > 0:
                headers[SentinelHubRateLimit.REQUEST_RETRY_HEADER] = int(1000 * expected_request_wait_time)

            expected_units_wait_time = max(
                bucket.get_expected_wait_time(0, 1, self.units_per_request, 0, 0) for bucket in self.policy_buckets
                if not bucket.is_request_bucket()
            )
            if expected_units_wait_time > 0:
                headers[SentinelHubRateLimit.UNITS_RETRY_HEADER] = int(1000 * expected_units_wait_time)

        return headers


class TestRateLimit(unittest.TestCase):
    """ A class that tests SentinelHubRateLimit class
    """
    @classmethod
    def setUp(cls):

        cls.test_cases = [
            TestCaseContainer('Trial policy', [
                    PolicyBucket(PolicyType.PROCESSING_UNITS, {
                        "capacity": 30000,
                        "samplingPeriod": "PT744H",
                        "nanosBetweenRefills": 89280000000
                    }),
                    PolicyBucket(PolicyType.PROCESSING_UNITS, {
                        "capacity": 300,
                        "samplingPeriod": "PT1M",
                        "nanosBetweenRefills": 200000000,
                    }),
                    PolicyBucket(PolicyType.REQUESTS, {
                        "capacity": 30000,
                        "samplingPeriod": "PT744H",
                        "nanosBetweenRefills": 89280000000,
                    }),
                    PolicyBucket(PolicyType.REQUESTS, {
                        "capacity": 300,
                        "samplingPeriod": "PT1M",
                        "nanosBetweenRefills": 200000000,
                    })
                ], units_per_request=25, process_time=0.1, request_num=100)
        ]

    def test_scenarios(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                policy_buckets = test_case.request

                rate_limit = SentinelHubRateLimit(None)
                rate_limit.policy_buckets = copy.deepcopy(policy_buckets)

                service = DummyService(
                    policy_buckets,
                    units_per_request=test_case.units_per_request,
                    process_time=test_case.process_time
                )

                request_num = test_case.request_num

                start_time = time.monotonic()
                while request_num > 0:
                    sleep_time = rate_limit.register_next()
                    print('requests left:', request_num, 'sleep time:', sleep_time)
                    print(rate_limit.expected_process_num, rate_limit.expected_cost_per_request)
                    for idx, bucket in enumerate(service.policy_buckets):
                        print(idx, bucket)

                    if sleep_time > 0:
                        time.sleep(sleep_time)
                        continue

                    response_headers = service.make_request()
                    if SentinelHubRateLimit.VIOLATION_HEADER not in response_headers:
                        request_num -= 1
                    print(response_headers)
                    rate_limit.update(response_headers)

                elapsed_time = time.monotonic() - start_time

                print(elapsed_time)


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
                              requests_completed=3000, wait_time=937.44),
            TestCaseContainer('Processing units bucket',
                              PolicyBucket('PROCESSING_UNITS', {"capacity": 300,
                                                                "samplingPeriod": "PT1M",
                                                                "nanosBetweenRefills": 200000000,
                                                                "niceSamplindPeriod": "1 minute"}),
                              is_request_bucket=False, is_fixed=False,
                              elapsed_time=10, new_content=200, cost_per_second=15.0,
                              requests_completed=50, wait_time=2.1),
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

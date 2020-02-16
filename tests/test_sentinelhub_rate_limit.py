"""
Tests for utilities that implement rate-limiting in the package
"""
import unittest
import copy
import concurrent.futures
import itertools as it
import time
from threading import Lock

from sentinelhub import TestSentinelHub, TestCaseContainer
from sentinelhub.sentinelhub_rate_limit import SentinelHubRateLimit, PolicyBucket, PolicyType


class DummyService:
    """ A class that simulates how Sentinel Hub service manages bucket policies. It is intended only for testing
    purposes
    """
    def __init__(self, policy_buckets, units_per_request, process_time):
        """
        :param policy_buckets: A list of policy buckets on the service
        :type policy_buckets: list(PolicyBucket)
        :param units_per_request: Number of processing units each request would cost. It assumes that each request will
            cost the same amount of processing units.
        :type units_per_request: int
        :param process_time: A number of seconds it would take to process each request. It assumes that each request
            will take the same amount of time.
        :type process_time: float
        """
        self.policy_buckets = policy_buckets
        self.units_per_request = units_per_request
        self.process_time = process_time

        self.time = time.monotonic()
        self.lock = Lock()

    def make_request(self):
        """ Simulates a single request to the service. First it waits the processing time, then it updates the policy
        buckets.
        """
        new_time = time.monotonic()
        time.sleep(self.process_time)
        elapsed_time = new_time - self.time

        self.lock.acquire()
        for bucket in self.policy_buckets:
            bucket.content = min(bucket.content + elapsed_time * bucket.refill_per_second, bucket.capacity)

        self.time = new_time

        new_content_list = self._get_new_bucket_content()
        is_rate_limited = min(new_content_list) < 0

        if not is_rate_limited:
            for bucket, new_content in zip(self.policy_buckets, new_content_list):
                bucket.content = new_content

        headers = self._get_headers(is_rate_limited)

        self.lock.release()
        return headers

    def _get_new_bucket_content(self):
        """ Calculates the new content of buckets
        """
        costs = ((1 if bucket.is_request_bucket() else self.units_per_request)
                 for bucket in self.policy_buckets)

        return [bucket.content - cost for bucket, cost in zip(self.policy_buckets, costs)]

    def _get_headers(self, is_rate_limited):
        """ Creates and returns headers that Sentinel Hub service would return
        """
        headers = {}

        request_bucket_content = [bucket.content for bucket in self.policy_buckets if bucket.is_request_bucket()]
        units_bucket_content = [bucket.content for bucket in self.policy_buckets if not bucket.is_request_bucket()]

        for bucket_content, header_key in [(request_bucket_content, SentinelHubRateLimit.REQUEST_COUNT_HEADER),
                                           (units_bucket_content, SentinelHubRateLimit.UNITS_COUNT_HEADER)]:
            if bucket_content:
                headers[header_key] = min(bucket_content)

        if is_rate_limited:
            headers[SentinelHubRateLimit.VIOLATION_HEADER] = True

            expected_request_wait_time = max(
                bucket.get_wait_time(0, 1, 1, 0, 0) for bucket in self.policy_buckets
                if bucket.is_request_bucket()
            )
            if expected_request_wait_time > 0:
                headers[SentinelHubRateLimit.REQUEST_RETRY_HEADER] = int(1000 * expected_request_wait_time)

            expected_units_wait_time = max(
                bucket.get_wait_time(0, 1, self.units_per_request, 0, 0) for bucket in self.policy_buckets
                if not bucket.is_request_bucket()
            )
            if expected_units_wait_time > 0:
                headers[SentinelHubRateLimit.UNITS_RETRY_HEADER] = int(1000 * expected_units_wait_time)

        return headers


class TestRateLimit(TestSentinelHub):
    """ A class that tests SentinelHubRateLimit class
    """
    @classmethod
    def setUp(cls):
        trial_policy_buckets = [
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
        ]

        small_policy_buckets = [
            PolicyBucket(PolicyType.REQUESTS, {
                "capacity": 5,
                "samplingPeriod": "PT1S",
                "nanosBetweenRefills": 200000000,
            }),
            PolicyBucket(PolicyType.PROCESSING_UNITS, {
                "capacity": 10,
                "samplingPeriod": "PT1S",
                "nanosBetweenRefills": 100000000,
            })
        ]

        fixed_bucket = PolicyBucket(PolicyType.REQUESTS, {
            "capacity": 10,
            "samplingPeriod": "PT0S",
            "nanosBetweenRefills": 9223372036854775807,
        })

        cls.test_cases = [
            TestCaseContainer('Trial policy, no hits', trial_policy_buckets, process_num=5, units_per_request=5,
                              process_time=0.5, request_num=10, max_elapsed_time=6, max_rate_limit_hits=0),
            TestCaseContainer('Trial policy, unit hits', trial_policy_buckets, process_num=5, units_per_request=5,
                              process_time=0.5, request_num=14, max_elapsed_time=12, max_rate_limit_hits=10),
            TestCaseContainer('Small policies', small_policy_buckets, process_num=3, units_per_request=2,
                              process_time=0.1, request_num=5, max_elapsed_time=3, max_rate_limit_hits=15),
            TestCaseContainer('Fixed policies', [fixed_bucket], process_num=2, units_per_request=20,
                              process_time=0.0, request_num=5, max_elapsed_time=0.6, max_rate_limit_hits=0),
        ]

    def test_scenarios(self):
        """ For each test case it simulates a parallel interaction between a service and multiple instances of
        rate-limiting object.
        """
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                policy_buckets = test_case.request
                process_num = test_case.process_num
                request_num = test_case.request_num

                rate_limit_objects = [SentinelHubRateLimit(num_processes=process_num) for _ in range(process_num)]

                service = DummyService(
                    copy.deepcopy(policy_buckets),
                    units_per_request=test_case.units_per_request,
                    process_time=test_case.process_time
                )

                start_time = time.monotonic()
                with concurrent.futures.ThreadPoolExecutor(max_workers=process_num) as executor:
                    results = list(executor.map(
                        self.run_interaction,
                        it.repeat(service),
                        rate_limit_objects,
                        it.repeat(request_num),
                        range(process_num)
                    ))
                elapsed_time = time.monotonic() - start_time
                total_rate_limit_hits = sum(results)

                self.assertLessEqual(elapsed_time, test_case.max_elapsed_time, msg='Rate limit object is too careful')
                self.assertLessEqual(total_rate_limit_hits, test_case.max_rate_limit_hits,
                                     msg='Rate limit object hit the rate limit too many times')

    def run_interaction(self, service, rate_limit, request_num, index):
        """ Runs an interaction between service instance and a single instance of a rate-limiting object
        """
        rate_limit_hits = 0
        while request_num > 0:
            sleep_time = rate_limit.register_next()

            if sleep_time > 0:
                self.LOGGER.info('Process: %d, requests left: %d, sleep time: %0.2f', index, request_num, sleep_time)
                # for idx, bucket in enumerate(service.policy_buckets):
                #     self.LOGGER.info('Process %d: %s', index, bucket)

                time.sleep(sleep_time)
                continue

            response_headers = service.make_request()
            if SentinelHubRateLimit.VIOLATION_HEADER not in response_headers:
                request_num -= 1
            else:
                rate_limit_hits += 1
                self.LOGGER.info('Process %d: rate limit hit %s', index, response_headers)

            rate_limit.update(response_headers)

        return rate_limit_hits


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

                wait_time = bucket.get_wait_time(
                    test_case.elapsed_time,
                    process_num=2,
                    cost_per_request=10,
                    requests_completed=test_case.requests_completed
                )

                self.assertAlmostEqual(wait_time, test_case.wait_time, delta=1e-6)


if __name__ == '__main__':
    unittest.main()

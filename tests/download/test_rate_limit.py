"""
Tests for utilities that implement rate-limiting in the package
"""
import concurrent.futures
import itertools as it
import time
from dataclasses import dataclass
from logging import Logger
from threading import Lock
from typing import Any, Dict, List, Tuple

import pytest
from pytest import approx

from sentinelhub.download.rate_limit import PolicyBucket, PolicyType, SentinelHubRateLimit
from sentinelhub.types import JsonDict


class DummyService:
    """A class that simulates how Sentinel Hub service manages bucket policies. It is intended only for testing
    purposes
    """

    def __init__(self, policy_buckets: List[PolicyBucket], units_per_request: float, process_time: float):
        """
        :param policy_buckets: A list of policy buckets on the service
        :param units_per_request: Number of processing units each request would cost. It assumes that each request will
            cost the same amount of processing units.
        :param process_time: A number of seconds it would take to process each request. It assumes that each request
            will take the same amount of time.
        """
        self.policy_buckets = policy_buckets
        self.units_per_request = units_per_request
        self.process_time = process_time

        self.time = time.monotonic()
        self.lock = Lock()

    def make_request(self) -> JsonDict:
        """Simulates a single request to the service. First it waits the processing time, then it updates the policy
        buckets.
        """
        new_time = time.monotonic()
        time.sleep(self.process_time)
        elapsed_time = new_time - self.time

        with self.lock:
            for bucket in self.policy_buckets:
                bucket.content = min(bucket.content + elapsed_time * bucket.refill_per_second, bucket.capacity)

            self.time = new_time

            new_content_list = self._get_new_bucket_content()
            is_rate_limited = min(new_content_list) < 0

            if not is_rate_limited:
                for bucket, new_content in zip(self.policy_buckets, new_content_list):
                    bucket.content = new_content

            headers = self._get_headers(is_rate_limited)

        return headers

    def _get_new_bucket_content(self) -> List[float]:
        """Calculates the new content of buckets"""
        costs = ((1 if bucket.is_request_bucket() else self.units_per_request) for bucket in self.policy_buckets)

        return [bucket.content - cost for bucket, cost in zip(self.policy_buckets, costs)]

    def _get_headers(self, is_rate_limited: bool) -> JsonDict:
        """Creates and returns headers that Sentinel Hub service would return"""
        headers = {SentinelHubRateLimit.UNITS_SPENT_HEADER: self.units_per_request}

        if is_rate_limited:
            expected_request_wait_time = max(
                bucket.get_wait_time(0, 1, 1, 0, 0) for bucket in self.policy_buckets if bucket.is_request_bucket()
            )
            expected_units_wait_time = max(
                bucket.get_wait_time(0, 1, self.units_per_request, 0, 0)
                for bucket in self.policy_buckets
                if not bucket.is_request_bucket()
            )
            expected_wait_time = max(expected_request_wait_time, expected_units_wait_time)

            headers[SentinelHubRateLimit.RETRY_HEADER] = str(int(1000 * expected_wait_time))

        return headers


TRIAL_POLICY_BUCKETS = [
    (PolicyType.PROCESSING_UNITS, dict(capacity=30000, samplingPeriod="PT744H", nanosBetweenRefills=89280000000)),
    (PolicyType.PROCESSING_UNITS, dict(capacity=300, samplingPeriod="PT1M", nanosBetweenRefills=200000000)),
    (PolicyType.REQUESTS, dict(capacity=30000, samplingPeriod="PT744H", nanosBetweenRefills=89280000000)),
    (PolicyType.REQUESTS, dict(capacity=300, samplingPeriod="PT1M", nanosBetweenRefills=200000000)),
]

SMALL_POLICY_BUCKETS = [
    (PolicyType.REQUESTS, dict(capacity=5, samplingPeriod="PT1S", nanosBetweenRefills=200000000)),
    (PolicyType.PROCESSING_UNITS, dict(capacity=10, samplingPeriod="PT1S", nanosBetweenRefills=100000000)),
]


FIXED_BUCKETS = [
    (PolicyType.REQUESTS, dict(capacity=10, samplingPeriod="PT0S", nanosBetweenRefills=9223372036854775807))
]


@pytest.mark.parametrize(
    "bucket_defs, process_num, units_per_request, process_time, request_num, max_elapsed_time, max_rate_limit_hits",
    [
        (TRIAL_POLICY_BUCKETS, 5, 5, 0.5, 10, 6, 0),
        (TRIAL_POLICY_BUCKETS, 5, 5, 0.5, 14, 12, 10),
        (SMALL_POLICY_BUCKETS, 3, 2, 0.1, 5, 3, 18),
        (FIXED_BUCKETS, 2, 20, 0.0, 5, 0.6, 0),
    ],
)
def test_scenarios(
    logger: Logger,
    bucket_defs: List[Tuple[PolicyType, Dict[str, Any]]],
    process_num: int,
    units_per_request: float,
    process_time: float,
    request_num: int,
    max_elapsed_time: float,
    max_rate_limit_hits: int,
) -> None:
    """For each test case it simulates a parallel interaction between a service and multiple instances of
    rate-limiting object.
    """
    rate_limit_objects = [SentinelHubRateLimit(num_processes=process_num) for _ in range(process_num)]

    service = DummyService(
        [PolicyBucket(kind, kwargs) for kind, kwargs in bucket_defs],
        units_per_request=units_per_request,
        process_time=process_time,
    )

    start_time = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=process_num) as executor:
        results = list(
            executor.map(
                run_interaction,  # custom function, defined below
                it.repeat(logger),
                it.repeat(service),
                rate_limit_objects,
                it.repeat(request_num),
                range(process_num),
            )
        )
    elapsed_time = time.monotonic() - start_time
    total_rate_limit_hits = sum(results)

    assert elapsed_time <= max_elapsed_time, "Rate limit object is too careful"
    assert total_rate_limit_hits <= max_rate_limit_hits, "Rate limit object hit the rate limit too many times"


def run_interaction(
    logger: Logger, service: DummyService, rate_limit: SentinelHubRateLimit, request_num: int, index: int
) -> int:
    """Runs an interaction between service instance and a single instance of a rate-limiting object"""
    rate_limit_hits = 0
    while request_num > 0:
        sleep_time = rate_limit.register_next()

        if sleep_time > 0:
            logger.info("Process: %d, requests left: %d, sleep time: %0.2f", index, request_num, sleep_time)
            # for idx, bucket in enumerate(service.policy_buckets):
            #     logger.info('Process %d: %s', index, bucket)

            time.sleep(sleep_time)
            continue

        response_headers = service.make_request()
        if SentinelHubRateLimit.RETRY_HEADER not in response_headers:
            request_num -= 1
        else:
            rate_limit_hits += 1
            logger.info("Process %d: rate limit hit %s", index, response_headers)

        rate_limit.update(response_headers)

    return rate_limit_hits


@dataclass
class PolicyBucketTestCase:
    bucket_kind: str
    bucket_kwargs: dict
    is_request_bucket: bool
    is_fixed: bool
    elapsed_time: float
    new_content: int
    cost_per_second: float
    requests_completed: int
    wait_time: float


TEST_CASES = [
    PolicyBucketTestCase(
        bucket_kind="REQUESTS",
        bucket_kwargs=dict(
            capacity=30000, samplingPeriod="PT744H", nanosBetweenRefills=89280000000, niceSamplingPeriod="31 days"
        ),
        is_request_bucket=True,
        is_fixed=False,
        elapsed_time=10,
        new_content=20000,
        cost_per_second=1000.0112007,
        requests_completed=3000,
        wait_time=937.44,
    ),
    PolicyBucketTestCase(
        bucket_kind="PROCESSING_UNITS",
        bucket_kwargs=dict(
            capacity=300, samplingPeriod="PT1M", nanosBetweenRefills=200000000, niceSamplingPeriod="1 minute"
        ),
        is_request_bucket=False,
        is_fixed=False,
        elapsed_time=10,
        new_content=200,
        cost_per_second=15.0,
        requests_completed=50,
        wait_time=2.1,
    ),
    PolicyBucketTestCase(
        bucket_kind="REQUESTS",
        bucket_kwargs=dict(
            capacity=10,
            samplingPeriod="PT0S",
            nanosBetweenRefills=9223372036854775807,
            niceSamplingPeriod="Fixed amount of tokens",
        ),
        is_request_bucket=True,
        is_fixed=True,
        elapsed_time=10,
        new_content=5,
        cost_per_second=0.5,
        requests_completed=10,
        wait_time=-1,
    ),
]


@pytest.mark.parametrize("test_case", TEST_CASES)
def test_basic_bucket_methods(test_case: PolicyBucketTestCase) -> None:
    bucket = PolicyBucket(test_case.bucket_kind, test_case.bucket_kwargs)

    assert bucket.is_request_bucket() == test_case.is_request_bucket
    assert bucket.is_fixed() == test_case.is_fixed
    assert repr(bucket).startswith("PolicyBucket(")

    original_content = bucket.content
    bucket.content = 0
    assert bucket.content == 0
    bucket.content = original_content
    assert bucket.content == original_content

    real_cost_per_second = bucket.count_cost_per_second(test_case.elapsed_time, test_case.new_content)
    assert real_cost_per_second == approx(test_case.cost_per_second, abs=1e-6)

    wait_time = bucket.get_wait_time(
        test_case.elapsed_time, process_num=2, cost_per_request=10, requests_completed=test_case.requests_completed
    )
    assert wait_time == approx(test_case.wait_time, abs=1e-6)

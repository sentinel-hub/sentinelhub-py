"""
Module implementing rate limiting logic for Sentinel Hub service
"""
import time
from enum import Enum

from ..types import JsonDict


class PolicyType(Enum):
    """Enum defining different types of policies"""

    PROCESSING_UNITS = "PROCESSING_UNITS"
    REQUESTS = "REQUESTS"


class SentinelHubRateLimit:
    """Class implementing rate limiting logic of Sentinel Hub service

    It has 2 public methods:

    - register_next - tells if next download can start or if not, what is the wait before it can be asked again
    - update - updates expectations according to headers obtained from download

    The rate limiting object is collecting information about the status of rate limiting policy buckets from
    Sentinel Hub service. According to this information and a feedback from download requests it adapts expectations
    about when the next download attempt will be possible.
    """

    RETRY_HEADER = "Retry-After"
    UNITS_SPENT_HEADER = "X-ProcessingUnits-Spent"

    def __init__(self, num_processes: int = 1, minimum_wait_time: float = 0.05, maximum_wait_time: float = 60.0):
        """
        :param num_processes: Number of parallel download processes running.
        :param minimum_wait_time: Minimum wait time between two consecutive download requests in seconds.
        :param maximum_wait_time: Maximum wait time between two consecutive download requests in seconds.
        """
        self.wait_time = min(num_processes * minimum_wait_time, maximum_wait_time)
        self.next_download_time = time.monotonic()

    def register_next(self) -> float:
        """Determines if next download request can start or not by returning the waiting time in seconds."""
        current_time = time.monotonic()
        wait_time = max(self.next_download_time - current_time, 0)

        if wait_time == 0:
            self.next_download_time = max(current_time + self.wait_time, self.next_download_time)

        return wait_time

    def update(self, headers: dict) -> None:
        """Update the next possible download time if the service has responded with the rate limit"""
        retry_after: float = round(headers.get(self.RETRY_HEADER, 0))
        retry_after = retry_after / 1000

        if retry_after:
            self.next_download_time = max(time.monotonic() + retry_after, self.next_download_time)


class PolicyBucket:
    """A class representing Sentinel Hub policy bucket"""

    def __init__(self, policy_type: PolicyType, policy_payload: JsonDict):
        """
        :param policy_type: A type of policy
        :param policy_payload: A dictionary of policy parameters
        """

        self.policy_type = PolicyType(policy_type)

        self.capacity = float(policy_payload["capacity"])
        self.refill_period = policy_payload["samplingPeriod"]

        # The following is the same as if we would interpret samplingPeriod string
        self.refill_per_second = 10**9 / policy_payload["nanosBetweenRefills"]

        self.content = self.capacity

    def __repr__(self) -> str:
        """Representation of the bucket content"""
        return (
            f"{self.__class__.__name__}(policy_type={self.policy_type}, content={self.content}/{self.capacity}, "
            f"refill_period={self.refill_period}, refill_per_second={self.refill_per_second})"
        )

    def count_cost_per_second(self, elapsed_time: float, new_content: float) -> float:
        """Calculates the cost per second for the bucket given the elapsed time and the new content.

        In the calculation it assumes that during the elapsed time bucket was being filled all the time - i.e. it
        assumes the bucket has never been full for a non-zero amount of time in the elapsed time period.
        """
        content_difference = self.content - new_content
        if not self.is_fixed():
            content_difference += elapsed_time * self.refill_per_second

        return content_difference / elapsed_time

    def get_wait_time(
        self,
        elapsed_time: float,
        process_num: int,
        cost_per_request: float,
        requests_completed: int,
        buffer_cost: float = 0.5,
    ) -> float:
        """Expected time a user would have to wait for this bucket"""
        overall_completed_cost = requests_completed * cost_per_request * process_num
        expected_content = max(self.content + elapsed_time * self.refill_per_second - overall_completed_cost, 0)

        if self.is_fixed():
            return -1 if expected_content < cost_per_request else 0

        return max(cost_per_request - expected_content + buffer_cost, 0) / self.refill_per_second

    def is_request_bucket(self) -> bool:
        """Checks if bucket counts requests"""
        return self.policy_type is PolicyType.REQUESTS

    def is_fixed(self) -> bool:
        """Checks if bucket has a fixed number of requests"""
        return self.refill_period == "PT0S"

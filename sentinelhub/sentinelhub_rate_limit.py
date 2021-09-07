"""
Module implementing rate limiting logic for Sentinel Hub service
"""
import time

from enum import Enum


class SentinelHubRateLimit:
    """ Class implementing rate limiting logic of Sentinel Hub service

    It has 2 public methods:

    - register_next - tells if next download can start or if not, what is the wait before it can be asked again
    - update - updates expectations according to headers obtained from download

    The rate limiting object is collecting information about the status of rate limiting policy buckets from
    Sentinel Hub service. According to this information and a feedback from download requests it adapts expectations
    about when the next download attempt will be possible.
    """

    REQUEST_RETRY_HEADER = 'Retry-After'
    REQUEST_COUNT_HEADER = 'X-RateLimit-Remaining'
    UNITS_RETRY_HEADER = 'X-ProcessingUnits-Retry-After'
    UNITS_COUNT_HEADER = 'X-ProcessingUnits-Remaining'
    VIOLATION_HEADER = 'X-RateLimit-ViolatedPolicy'

    def __init__(self, num_processes=1, minimum_wait_time=0.05, maximum_wait_time=60.0):
        """
        :param num_processes: Number of parallel download processes running.
        :type num_processes: int
        :param minimum_wait_time: Minimum wait time between two consecutive download requests in seconds.
        :type minimum_wait_time: float
        :param maximum_wait_time: Maximum wait time between two consecutive download requests in seconds.
        :type maximum_wait_time: float
        """
        self.wait_time = min(num_processes * minimum_wait_time, maximum_wait_time)
        self.next_download_time = time.monotonic()

    def register_next(self):
        """ Determines if next download request can start or not by returning the waiting time in seconds.
        """
        current_time = time.monotonic()
        wait_time = max(self.next_download_time - current_time, 0)

        if wait_time == 0:
            self.next_download_time = max(current_time + self.wait_time, self.next_download_time)

        return wait_time

    def update(self, headers):
        """ Update the next possible download time if the service has responded with the rate limit
        """

        retry_after = max(int(headers.get(self.REQUEST_RETRY_HEADER, 0)), int(headers.get(self.UNITS_RETRY_HEADER, 0)))
        retry_after = retry_after / 1000

        if retry_after:
            self.next_download_time = max(time.monotonic() + retry_after, self.next_download_time)


class PolicyBucket:
    """ A class representing Sentinel Hub policy bucket
    """
    def __init__(self, policy_type, policy_payload):
        """
        :param policy_type: A type of policy
        :type policy_type: PolicyType or str
        :param policy_payload: A dictionary of policy parameters
        :type policy_payload: dict
        """

        self.policy_type = PolicyType(policy_type)

        self.capacity = float(policy_payload['capacity'])
        self.refill_period = policy_payload['samplingPeriod']

        # The following is the same as if we would interpret samplingPeriod string
        self.refill_per_second = 10 ** 9 / policy_payload['nanosBetweenRefills']

        self._content = self.capacity

    def __repr__(self):
        """ Representation of the bucket content
        """
        return (f'{self.__class__.__name__}(policy_type={self.policy_type}, content={self.content}/{self.capacity}, '
                f'refill_period={self.refill_period}, refill_per_second={self.refill_per_second})')

    @property
    def content(self):
        """ Variable `content` can be accessed as a property
        """
        return self._content

    @content.setter
    def content(self, value):
        """ Variable `content` can be modified by external classes
        """
        self._content = value

    def count_cost_per_second(self, elapsed_time, new_content):
        """ Calculates the cost per second for the bucket given the elapsed time and the new content.

        In the calculation it assumes that during the elapsed time bucket was being filled all the time - i.e. it
        assumes the bucket has never been full for a non-zero amount of time in the elapsed time period.
        """
        content_difference = self.content - new_content
        if not self.is_fixed():
            content_difference += elapsed_time * self.refill_per_second

        return content_difference / elapsed_time

    def get_wait_time(self, elapsed_time, process_num, cost_per_request, requests_completed, buffer_cost=0.5):
        """ Expected time a user would have to wait for this bucket
        """
        overall_completed_cost = requests_completed * cost_per_request * process_num
        expected_content = max(self.content + elapsed_time * self.refill_per_second - overall_completed_cost, 0)

        if self.is_fixed():
            if expected_content < cost_per_request:
                return -1
            return 0

        return max(cost_per_request - expected_content + buffer_cost, 0) / self.refill_per_second

    def is_request_bucket(self):
        """ Checks if bucket counts requests
        """
        return self.policy_type is PolicyType.REQUESTS

    def is_fixed(self):
        """ Checks if bucket has a fixed number of requests
        """
        return self.refill_period == 'PT0S'


class PolicyType(Enum):
    """ Enum defining different types of policies
    """
    PROCESSING_UNITS = 'PROCESSING_UNITS'
    REQUESTS = 'REQUESTS'

"""
Module implementing rate limiting logic for Sentinel Hub service
"""
import math
import time
from enum import Enum

from .download.client import get_json
from .exceptions import OutOfRequestsException


ERR = 0.01

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

    def __init__(self, session, *, status_refresh_period=60):
        """
        :param session: An instance of Sentinel Hub session object
        :type session: sentinelhub.SentinelHubSession or None
        :param status_refresh_period: Number of seconds between two consecutive rate limit status checks
        :type status_refresh_period: int
        """
        self.session = session
        self.status_refresh_period = None if self.session is None else status_refresh_period

        self.requests_completed = 0  # This counts completed requests only in a single status check period
        self.requests_in_process = 0

        # Initial expectations:
        self.expected_process_num = 1
        self.expected_cost_per_request = 1

        self.last_status_update_time = time.monotonic() if self.session is None else None
        self.policy_buckets = self._initialize_policy_buckets()

    def _initialize_policy_buckets(self):
        """ Collects and prepares a list of policy buckets defined for a user for which a given session has been
        created
        """
        if self.session is None:
            return self._initialize_trial_policy_buckets()

        user_policies = self._fetch_user_policies()

        if not user_policies['data']:
            user_policies = self._fetch_user_policies(default_policies=True)

        buckets = []
        for policy_payload in user_policies['data']:

            policy_info = policy_payload['type'] if 'type' in policy_payload else policy_payload
            policy_type = policy_info['name']

            policies = policy_payload.get('policies')  # Can either be None or an empty list
            if not policies:
                policies = policy_info['defaultPolicies']

            for policy in policies:
                buckets.append(PolicyBucket(policy_type, policy))

        return buckets

    @staticmethod
    def _initialize_trial_policy_buckets():
        """ Prepares default trial policy buckets
        """
        return [
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

    def _fetch_user_policies(self, default_policies=False):
        """ Collects user rate limiting policies
        """
        url = '{}/contract'.format(self.session.config.get_sh_rate_limit_url())
        if default_policies:
            url = '{}/types'.format(url)

        return get_json(url, headers=self.session.session_headers)

    def _fetch_status(self):
        """ Collects status about remaining requests and processing units
        """
        url = '{}/statistics/tokenCounts'.format(self.session.config.get_sh_rate_limit_url())

        return get_json(url, headers=self.session.session_headers)

    def register_next(self):
        """ Determines if next download request can start or not
        """
        if self.status_refresh_period is not None and \
                (self.last_status_update_time is None or
                 self.last_status_update_time + self.status_refresh_period <= time.monotonic()):
            self._refresh_status()

        elapsed_time = time.monotonic() - self.last_status_update_time

        max_wait_time = 0
        for bucket in self.policy_buckets:
            expected_cost_per_request = 1 if bucket.is_request_bucket() else self.expected_cost_per_request
            max_requests_completed = self.requests_completed + self.requests_in_process

            expected_wait_time = bucket.get_expected_wait_time(elapsed_time,
                                                               self.expected_process_num,
                                                               expected_cost_per_request,
                                                               max_requests_completed)
            if expected_wait_time == -1:
                raise OutOfRequestsException('Your user has run out of available number of requests')

            max_wait_time = max(max_wait_time, expected_wait_time)

        if max_wait_time == 0:
            self.requests_in_process += 1
        return max_wait_time

    def _refresh_status(self):
        """ Collects new status, updates expectations and updates buckets with the new content
        """
        new_status = self._fetch_status()

        new_status_update_time = time.monotonic()
        new_content_list = [new_status['data'][bucket.policy_type.value][bucket.refill_period]
                            for bucket in self.policy_buckets]

        if self.last_status_update_time is not None:
            elapsed_time = new_status_update_time - self.last_status_update_time
            self._update_expectations(elapsed_time, new_content_list)

        for bucket, new_content in zip(self.policy_buckets, new_content_list):
            bucket.content = new_content

        self.last_status_update_time = new_status_update_time
        self.requests_completed = 0

    def _update_expectations(self, elapsed_time, new_content_list):
        """ Updates expectation variables
        """

        cost_per_second_list = [bucket.count_cost_per_second(elapsed_time, new_content) for bucket, new_content in
                                zip(self.policy_buckets, new_content_list)]

        requests_per_second = min(cost for cost, bucket in zip(cost_per_second_list, self.policy_buckets)
                                  if bucket.is_request_bucket())
        cost_per_second = min(cost for cost, bucket in zip(cost_per_second_list, self.policy_buckets)
                              if not bucket.is_request_bucket())

        cost_per_request = cost_per_second / max(requests_per_second, ERR)

        self._update_expected_process_num(elapsed_time * requests_per_second)
        self._update_expected_cost_per_request(cost_per_request)

    def update(self, headers):
        """ Update expectations by using information from response headers
        """
        elapsed_time = time.monotonic() - self.last_status_update_time
        is_rate_limited = self.VIOLATION_HEADER in headers

        self.requests_in_process -= 1
        if not is_rate_limited:
            self.requests_completed += 1

        if self.REQUEST_COUNT_HEADER not in headers:
            return

        remaining_requests = float(headers[self.REQUEST_COUNT_HEADER])
        requests_per_second = max(bucket.count_cost_per_second(elapsed_time, remaining_requests) for bucket in
                                  self.policy_buckets if bucket.is_request_bucket())

        self._update_expected_process_num(elapsed_time * requests_per_second, is_rate_limited=is_rate_limited)

        if self.UNITS_COUNT_HEADER not in headers:
            return

        remaining_units = float(headers[self.UNITS_COUNT_HEADER])
        cost_per_second = max(bucket.count_cost_per_second(elapsed_time, remaining_units) for bucket in
                              self.policy_buckets if not bucket.is_request_bucket())
        cost_per_request = cost_per_second / max(requests_per_second, ERR)

        self._update_expected_cost_per_request(cost_per_request, is_rate_limited=is_rate_limited)

    def _update_expected_process_num(self, requests_done, is_rate_limited=False):
        """ This method updates the expected number of concurrent processes using this credentials
        """
        # Note: if self.requests_completed is small the following is likely to be inaccurate
        max_process_num = math.ceil(requests_done / max(self.requests_completed + self.requests_in_process, 1))
        min_process_num = math.floor(requests_done / max(self.requests_completed + self.requests_in_process, 1))

        if is_rate_limited or self.expected_process_num < min_process_num:
            self.expected_process_num += 1

        if self.expected_process_num > max_process_num:
            self.expected_process_num -= 1

        self.expected_process_num = max(self.expected_process_num, 1)

    def _update_expected_cost_per_request(self, cost_per_request, is_rate_limited=False):
        """ This method updates the expected cost per request
        """
        value_difference = cost_per_request - self.expected_cost_per_request

        if is_rate_limited:
            # Making sure we cannot decrease expectations if we get rate-limited
            value_difference = max(value_difference, 1)
        else:
            value_difference = min(max(value_difference, -1), 1) / 2

        self.expected_cost_per_request += value_difference
        self.expected_cost_per_request = max(self.expected_cost_per_request, 1)


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
        return '{}(policy_type={}, content={}/{}, refill_period={}, refill_per_second={})' \
               ''.format(self.__class__.__name__, self.policy_type, self.content, self.capacity, self.refill_period,
                         self.refill_per_second)

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
        assumes the bucket has never been ful for a non-zero amount of time in the elapsed time period.
        """
        content_difference = self.content - new_content
        if not self.is_fixed():
            content_difference += elapsed_time * self.refill_per_second

        return content_difference / elapsed_time

    def get_expected_wait_time(self, elapsed_time, expected_process_num, expected_cost_per_request,
                               requests_completed, buffer_cost=0.5):
        """ Expected time a user would have to wait for this bucket
        """
        overall_completed_cost = requests_completed * expected_cost_per_request * expected_process_num
        expected_content = self.content + elapsed_time * self.refill_per_second - overall_completed_cost

        if self.is_fixed():
            if expected_content < expected_cost_per_request:
                return -1
            return 0

        return max(expected_cost_per_request - expected_content + buffer_cost, 0) / self.refill_per_second

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

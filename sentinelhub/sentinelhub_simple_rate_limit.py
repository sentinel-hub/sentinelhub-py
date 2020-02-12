"""
Module implementing simple rate limiting logic for Sentinel Hub service
"""
import time


class SentinelSimpleRateLimit:

    REQUEST_RETRY_HEADER = 'Retry-After'
    REQUEST_COUNT_HEADER = 'X-RateLimit-Remaining'
    UNITS_RETRY_HEADER = 'X-ProcessingUnits-Retry-After'
    UNITS_COUNT_HEADER = 'X-ProcessingUnits-Remaining'
    VIOLATION_HEADER = 'X-RateLimit-ViolatedPolicy'

    def __init__(self, num_processes=1, minimum_wait_time=0.05, maximum_wait_time=60):
        self.wait_time = min(num_processes * minimum_wait_time, maximum_wait_time)
        self.next_download_time = time.monotonic()

    def register_next(self):
        current_time = time.monotonic()
        wait_time = max(self.next_download_time - current_time, 0)

        if wait_time == 0:
            self.next_download_time = max(current_time + self.wait_time, self.next_download_time)

        return wait_time

    def update(self, headers):
        """ Update expectations by using information from response headers
        """

        retry_after = max(headers.get(self.REQUEST_RETRY_HEADER, 0), headers.get(self.UNITS_RETRY_HEADER, 0))
        retry_after = retry_after / 1000

        if retry_after:
            self.next_download_time = max(time.monotonic() + retry_after, self.next_download_time)

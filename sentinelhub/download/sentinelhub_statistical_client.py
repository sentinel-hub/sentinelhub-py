"""
Download process for Sentinel Hub Statistical API
"""
import concurrent.futures
import copy
import logging
import os
from typing import Any, Dict, cast

from ..constants import MimeType
from ..decoding import decode_data as decode_data_function
from ..io_utils import read_data, write_data
from ..type_utils import JsonDict
from .request import DownloadRequest
from .sentinelhub_client import SentinelHubDownloadClient

LOGGER = logging.getLogger(__name__)


class SentinelHubStatisticalDownloadClient(SentinelHubDownloadClient):
    """A special download client for Sentinel Hub Statistical API

    Besides a normal download from Sentinel Hub services it implements an additional process of retrying and caching
    """

    _RETRIABLE_ERRORS = ["EXECUTION_ERROR", "TIMEOUT"]

    def __init__(self, *args: Any, n_interval_retries: int = 1, max_retry_threads: int = 5, **kwargs: Any):
        """
        :param n_interval_retries: Number of retries if a request fails just for a certain timestamp. (This parameter
            is experimental and might be changed in the future.)
        :param max_retry_threads: Number of threads used for retrying. (This parameter is experimental and might be
            changed in the future.)
        """
        super().__init__(*args, **kwargs)

        self.n_interval_retries = n_interval_retries
        self.max_retry_threads = max_retry_threads

    def _single_download(self, request: DownloadRequest, _: Any) -> Any:
        """Method for downloading a single request"""
        request.raise_if_invalid()
        if not (request.save_response or request.return_data):
            return None

        request_path, response_path = request.get_storage_paths()

        download_required = self.redownload or response_path is None or not os.path.exists(response_path)
        if download_required:
            response_content = self._execute_download(request)
            stats_response = decode_data_function(response_content, request.data_type)
        else:
            path = cast(str, response_path)  # fix in future, for now ensured by download_required
            LOGGER.debug("Reading locally stored data from %s instead of downloading", path)
            self._check_cached_request_is_matching(request, request_path)
            stats_response = read_data(path, data_format=request.data_type)

        failed_time_intervals: Dict[int, Any] = {}
        for index, stat_info in enumerate(stats_response["data"]):
            if self._has_retriable_error(stat_info):
                failed_time_intervals[index] = stat_info["interval"]

        n_succeeded_intervals = 0
        if failed_time_intervals:
            LOGGER.debug("Failed for %s intervals, retrying by downloading per interval", len(failed_time_intervals))
            retried_responses = self._download_per_interval(request, failed_time_intervals)
            n_succeeded_intervals = sum("error" not in stat_info for stat_info in retried_responses.values())

            stats_response["data"] = [
                retried_responses.get(index, stat_info) for index, stat_info in enumerate(stats_response["data"])
            ]

        if request_path and request.save_response and (self.redownload or not os.path.exists(request_path)):
            request_info = request.get_request_params(include_metadata=True)
            write_data(request_path, request_info, MimeType.JSON)
            LOGGER.debug("Saved request info to %s", request_path)

        if request.save_response and (download_required or n_succeeded_intervals > 0):
            path = cast(str, response_path)  # fix in future, for now ensured by raise_if_invalid
            write_data(path, stats_response, data_format=request.data_type)
            LOGGER.debug("Saved data to %s", response_path)

        if request.return_data:
            return stats_response
        return None

    def _download_per_interval(self, request: DownloadRequest, time_intervals: Dict[int, Any]) -> dict:
        """Download statistics per each time interval"""
        interval_requests = []
        for time_interval in time_intervals.values():
            interval_request = copy.deepcopy(request)
            if interval_request.post_values is None or "aggregation" not in interval_request.post_values:
                raise ValueError("Unable to configure request for retrying by interval.")

            interval_request.post_values["aggregation"]["timeRange"] = time_interval
            interval_requests.append(interval_request)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_retry_threads) as executor:
            stat_info_responses = list(executor.map(self._execute_single_stat_download, interval_requests))

        return dict(zip(time_intervals, stat_info_responses))

    def _execute_single_stat_download(self, request: DownloadRequest) -> Any:
        """Makes sure a download for a single time interval is retried"""
        for retry_count in range(self.n_interval_retries):
            response = self._execute_download(request)
            stat_response = decode_data_function(response, request.data_type)
            stat_info = stat_response["data"][0]

            if not self._has_retriable_error(stat_info) or retry_count == self.n_interval_retries - 1:
                return stat_info

        raise ValueError("No more retries available, download unsuccessful")

    def _has_retriable_error(self, stat_info: JsonDict) -> bool:
        """Checks if a dictionary of Stat API info for a single time interval has an error that can fixed by retrying
        a request
        """
        error_type = stat_info.get("error", {}).get("type")
        return error_type in self._RETRIABLE_ERRORS

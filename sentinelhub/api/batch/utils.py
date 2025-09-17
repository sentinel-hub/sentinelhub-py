"""
Module implementing utilities for working with batch jobs.
"""

from __future__ import annotations

import logging
import time
from typing import Union

from tqdm.auto import tqdm

from ...config import SHConfig
from ...types import JsonDict
from .base import BatchRequestStatus
from .process import BatchProcessClient, BatchProcessRequest
from .statistical import BatchStatisticalRequest, SentinelHubBatchStatistical

LOGGER = logging.getLogger(__name__)

BatchStatisticalRequestSpec = Union[str, dict, BatchStatisticalRequest]


_MIN_SLEEP_TIME = 60
_DEFAULT_SLEEP_TIME = 120
_MIN_STAT_SLEEP_TIME = 15
_DEFAULT_STAT_SLEEP_TIME = 30
_MIN_ANALYSIS_SLEEP_TIME = 5
_DEFAULT_ANALYSIS_SLEEP_TIME = 10


def monitor_batch_process_job(
    request: BatchProcessRequest,
    client: BatchProcessClient,
    sleep_time: int = _DEFAULT_SLEEP_TIME,
    analysis_sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
) -> BatchProcessRequest:
    """A utility function that keeps checking the progress of the batch processing job. Returns an updated version of
    the request

    Notes:

      - Before calling this function make sure to start a batch job by calling `BatchProcessingClient.start_job` method.
        In case a batch job is still being analysed this function will wait until the analysis ends.
      - This function will be continuously collecting information from Sentinel Hub service. To avoid making too many
        requests please make sure to adjust `sleep_time` parameter.

    :param request: The request to monitor.
    :param client: A batch processing client with appropriate configuration that is used to monitor the batch job.
    :param sleep_time: Number of seconds to sleep between consecutive progress bar updates.
    :param analysis_sleep_time: Number of seconds between consecutive status updates during analysis phase.
    """
    if sleep_time < _MIN_SLEEP_TIME:
        raise ValueError(f"To avoid making too many service requests please set sleep_time>={_MIN_SLEEP_TIME}")

    batch_request: BatchProcessRequest = monitor_batch_process_analysis(request, client, sleep_time=analysis_sleep_time)
    if batch_request.status is BatchRequestStatus.PROCESSING:
        LOGGER.info("Batch job is running")

    completion = batch_request.completion_percentage
    progress_bar = tqdm(total=100, initial=completion, desc="Completion percentage")

    monitoring_status = [BatchRequestStatus.ANALYSIS_DONE, BatchRequestStatus.PROCESSING]
    with progress_bar:
        while completion < 100 and batch_request.status in monitoring_status:
            time.sleep(sleep_time)
            batch_request = client.get_request(batch_request)
            progress_bar.update(batch_request.completion_percentage - completion)
            completion = batch_request.completion_percentage

    while batch_request.status in monitoring_status:
        LOGGER.info("Waiting on batch job status update, currently %s", batch_request.status)
        time.sleep(sleep_time)
        batch_request = client.get_request(batch_request)

    LOGGER.info("Batch job finished with status %s", batch_request.status.value)
    return batch_request


def monitor_batch_statistical_job(
    batch_request: BatchStatisticalRequestSpec,
    config: SHConfig | None = None,
    sleep_time: int = _DEFAULT_STAT_SLEEP_TIME,
    analysis_sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
) -> JsonDict:
    """A utility function that keeps checking the completion percentage of a Batch Statistical request until complete.

    Notes:

      - Before calling this function make sure to start a batch job via `SentinelHubBatchStatistical.start_job` method.
        In case a batch job is still being analysed this function will wait until the analysis ends.
      - Some information about the progress of this function is reported to logging level INFO.

    :param batch_request: An object with information about a batch request. Alternatively, it could only be a batch
        request id or a payload.
    :param config: A configuration object with required parameters `sh_client_id`, `sh_client_secret`, and
        `sh_auth_base_url` which is used for authentication and `sh_base_url` which defines the service deployment
        where Batch API will be called.
    :param sleep_time: Number of seconds to sleep between consecutive progress bar updates.
    :param analysis_sleep_time: Number of seconds between consecutive status updates during analysis phase.
    :return: Final status of the batch request.
    """
    if sleep_time < _MIN_STAT_SLEEP_TIME:
        raise ValueError(f"To avoid making too many service requests please set sleep_time>={_MIN_STAT_SLEEP_TIME}")

    batch_request = monitor_batch_statistical_analysis(batch_request, config, sleep_time=analysis_sleep_time)
    if batch_request.status is BatchRequestStatus.PROCESSING:
        LOGGER.info("Batch job is running")

    batch_client = SentinelHubBatchStatistical(config=config)

    request_status = batch_client.get_status(batch_request)
    progress = request_status["completionPercentage"]
    with tqdm(total=100, initial=progress, desc="Completion percentage") as progress_bar:
        while progress < 100:
            time.sleep(sleep_time)

            request_status = batch_client.get_status(batch_request)
            new_progress = request_status["completionPercentage"]
            progress_bar.update(new_progress - progress)
            progress = new_progress
    return request_status


def monitor_batch_process_analysis(
    request: BatchProcessRequest,
    client: BatchProcessClient,
    sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
) -> BatchProcessRequest:
    """A utility function that is waiting until analysis phase of a batch job finishes and regularly checks its status.
    In case analysis phase failed it raises an error at the end.

    :param request: The request to monitor.
    :param client: A batch processing client with appropriate configuration that is used to monitor the batch job.
    :param sleep_time: Number of seconds between consecutive status updates during analysis phase.
    """
    if sleep_time < _MIN_ANALYSIS_SLEEP_TIME:
        raise ValueError(
            f"To avoid making too many service requests please set analysis sleep time >={_MIN_ANALYSIS_SLEEP_TIME}"
        )

    batch_request = client.get_request(request)
    while batch_request.status in [BatchRequestStatus.CREATED, BatchRequestStatus.ANALYSING]:
        LOGGER.info("Batch job has a status %s, sleeping for %d seconds", batch_request.status.value, sleep_time)
        time.sleep(sleep_time)
        batch_request = client.get_request(batch_request)

    batch_request.raise_for_status(status=[BatchRequestStatus.FAILED, BatchRequestStatus.STOPPED])
    return batch_request


def monitor_batch_statistical_analysis(
    batch_request: BatchStatisticalRequestSpec,
    config: SHConfig | None = None,
    sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
) -> BatchStatisticalRequest:
    """A utility function that is waiting until analysis phase of a batch job finishes and regularly checks its status.
    In case analysis phase failed it raises an error at the end.

    :param batch_request: An object with information about a batch request. Alternatively, it could only be a batch
        request id or a payload.
    :param config: A configuration object with required parameters `sh_client_id`, `sh_client_secret`, and
        `sh_auth_base_url` which is used for authentication and `sh_base_url` which defines the service deployment
        where Batch API will be called.
    :param sleep_time: Number of seconds between consecutive status updates during analysis phase.
    :return: Batch request info
    """
    if sleep_time < _MIN_ANALYSIS_SLEEP_TIME:
        raise ValueError(
            f"To avoid making too many service requests please set analysis sleep time >={_MIN_ANALYSIS_SLEEP_TIME}"
        )

    batch_client = SentinelHubBatchStatistical(config=config)
    request_status = BatchRequestStatus(batch_client.get_status(batch_request)["status"])
    while request_status in [BatchRequestStatus.CREATED, BatchRequestStatus.ANALYSING]:
        LOGGER.info("Batch job has a status %s, sleeping for %d seconds", request_status.value, sleep_time)
        time.sleep(sleep_time)
        request_status = BatchRequestStatus(batch_client.get_status(batch_request)["status"])

    batch_request = batch_client.get_request(batch_request)
    batch_request.raise_for_status(status=[BatchRequestStatus.FAILED, BatchRequestStatus.STOPPED])
    return batch_request

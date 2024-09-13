"""
Module implementing utilities for working with batch jobs.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Union

from tqdm.auto import tqdm

from ...config import SHConfig
from ...types import JsonDict
from .base import BatchRequestStatus
from .process import BatchRequest, BatchTileStatus, SentinelHubBatch
from .process_v2 import BatchProcessClient, BatchProcessRequest
from .statistical import BatchStatisticalRequest, SentinelHubBatchStatistical

LOGGER = logging.getLogger(__name__)

BatchProcessRequestSpec = Union[str, dict, BatchRequest]
BatchStatisticalRequestSpec = Union[str, dict, BatchStatisticalRequest]


_MIN_SLEEP_TIME = 60
_DEFAULT_SLEEP_TIME = 120
_MIN_STAT_SLEEP_TIME = 15
_DEFAULT_STAT_SLEEP_TIME = 30
_MIN_ANALYSIS_SLEEP_TIME = 5
_DEFAULT_ANALYSIS_SLEEP_TIME = 10


def monitor_batch_job(
    batch_request: BatchProcessRequestSpec,
    config: SHConfig | None = None,
    sleep_time: int = _DEFAULT_SLEEP_TIME,
    analysis_sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
) -> defaultdict[BatchTileStatus, list[dict]]:
    """A utility function that keeps checking for number of processed tiles until the given batch request finishes.
    During the process it shows a progress bar and at the end it reports information about finished and failed tiles.

    Notes:

      - Before calling this function make sure to start a batch job by calling `SentinelHubBatch.start_job` method. In
        case a batch job is still being analysed this function will wait until the analysis ends.
      - This function will be continuously collecting tile information from Sentinel Hub service. To avoid making too
        many requests please make sure to adjust `sleep_time` parameter according to the size of your job. Larger jobs
        don't need very frequent tile status updates.
      - Some information about the progress of this function is reported to logging level INFO.

    :param batch_request: An object with information about a batch request. Alternatively, it could only be a batch
        request id or a payload.
    :param config: A configuration object with required parameters `sh_client_id`, `sh_client_secret`, and
        `sh_auth_base_url` which is used for authentication and `sh_base_url` which defines the service deployment
        where Batch API will be called.
    :param sleep_time: Number of seconds to sleep between consecutive progress bar updates.
    :param analysis_sleep_time: Number of seconds between consecutive status updates during analysis phase.
    :return: A dictionary mapping a tile status to a list of tile payloads.
    """
    if sleep_time < _MIN_SLEEP_TIME:
        raise ValueError(f"To avoid making too many service requests please set sleep_time>={_MIN_SLEEP_TIME}")

    batch_request = monitor_batch_analysis(batch_request, config=config, sleep_time=analysis_sleep_time)
    if batch_request.status is BatchRequestStatus.PROCESSING:
        LOGGER.info("Batch job is running")

    batch_client = SentinelHubBatch(config=config)

    tiles_per_status = _get_batch_tiles_per_status(batch_request, batch_client)
    success_count = len(tiles_per_status[BatchTileStatus.PROCESSED])
    finished_count = success_count + len(tiles_per_status[BatchTileStatus.FAILED])

    progress_bar = tqdm(total=batch_request.tile_count, initial=finished_count, desc="Progress rate")
    success_bar = tqdm(total=finished_count, initial=success_count, desc="Success rate")

    monitoring_status = [BatchRequestStatus.ANALYSIS_DONE, BatchRequestStatus.PROCESSING]
    with progress_bar, success_bar:
        while finished_count < batch_request.tile_count and batch_request.status in monitoring_status:
            time.sleep(sleep_time)
            batch_request = batch_client.get_request(batch_request)

            tiles_per_status = _get_batch_tiles_per_status(batch_request, batch_client)
            new_success_count = len(tiles_per_status[BatchTileStatus.PROCESSED])
            new_finished_count = new_success_count + len(tiles_per_status[BatchTileStatus.FAILED])

            progress_bar.update(new_finished_count - finished_count)
            if new_finished_count != finished_count:
                success_bar.total = new_finished_count
                success_bar.refresh()
            success_bar.update(new_success_count - success_count)

            finished_count = new_finished_count
            success_count = new_success_count

    failed_tiles_num = finished_count - success_count
    if failed_tiles_num:
        LOGGER.info("Batch job failed for %d tiles", failed_tiles_num)

    while batch_request.status is BatchRequestStatus.PROCESSING:
        LOGGER.info("Waiting on batch job status update.")
        time.sleep(sleep_time)
        batch_request = batch_client.get_request(batch_request)

    LOGGER.info("Batch job finished with status %s", batch_request.status.value)
    return tiles_per_status


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


def _get_batch_tiles_per_status(
    batch_request: BatchRequest, batch_client: SentinelHubBatch
) -> defaultdict[BatchTileStatus, list[dict]]:
    """A helper function that queries information about batch tiles and returns information about tiles, grouped by
    tile status.

    :return: A dictionary mapping a tile status to a list of tile payloads.
    """
    tiles_per_status = defaultdict(list)

    for tile in batch_client.iter_tiles(batch_request):
        status = BatchTileStatus(tile["status"])
        tiles_per_status[status].append(tile)

    return tiles_per_status


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


def monitor_batch_analysis(
    batch_request: BatchProcessRequestSpec,
    config: SHConfig | None = None,
    sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
) -> BatchRequest:
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

    batch_client = SentinelHubBatch(config=config)
    batch_request = batch_client.get_request(batch_request)
    while batch_request.status in [BatchRequestStatus.CREATED, BatchRequestStatus.ANALYSING]:
        LOGGER.info("Batch job has a status %s, sleeping for %d seconds", batch_request.status.value, sleep_time)
        time.sleep(sleep_time)
        batch_request = batch_client.get_request(batch_request)

    batch_request.raise_for_status(status=[BatchRequestStatus.FAILED, BatchRequestStatus.CANCELED])
    return batch_request


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

    batch_request.raise_for_status(status=[BatchRequestStatus.FAILED, BatchRequestStatus.CANCELED])
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
    batch_request.raise_for_status(status=[BatchRequestStatus.FAILED, BatchRequestStatus.CANCELED])
    return batch_request

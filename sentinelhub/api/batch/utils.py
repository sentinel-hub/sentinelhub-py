"""
Module implementing utilities for working with batch jobs.
"""
import logging
import sys
import time
import warnings
from collections import defaultdict
from typing import DefaultDict, List, Optional, TypeVar, Union, overload

from tqdm.auto import tqdm

from ...config import SHConfig
from .base import BatchRequestStatus
from .process import BatchRequest, BatchTileStatus, SentinelHubBatch
from .statistical import BatchStatisticalRequest, SentinelHubBatchStatistical

if sys.version_info < (3, 8):
    from typing_extensions import Literal
else:
    from typing import Literal  # pylint: disable=ungrouped-imports

LOGGER = logging.getLogger(__name__)

BatchRequestSpec = Union[str, dict, BatchRequest, BatchStatisticalRequest]
BatchRequestType = TypeVar("BatchRequestType", BatchStatisticalRequest, BatchRequest)  # pylint: disable=invalid-name
BatchKind = Literal["process", "statistical"]


_MIN_SLEEP_TIME = 60
_DEFAULT_SLEEP_TIME = 120
_MIN_ANALYSIS_SLEEP_TIME = 5
_DEFAULT_ANALYSIS_SLEEP_TIME = 10


@overload
def monitor_batch_job(
    batch_request: Union[str, dict],
    config: Optional[SHConfig],
    sleep_time: int = _DEFAULT_SLEEP_TIME,
    analysis_sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
    *,
    batch_kind: Literal["process"],
) -> DefaultDict[BatchTileStatus, List[dict]]:
    pass


@overload
def monitor_batch_job(
    batch_request: BatchRequest,
    config: Optional[SHConfig],
    sleep_time: int = _DEFAULT_SLEEP_TIME,
    analysis_sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
    *,
    batch_kind: BatchKind,
) -> DefaultDict[BatchTileStatus, List[dict]]:
    pass


@overload
def monitor_batch_job(
    batch_request: Union[str, dict],
    config: Optional[SHConfig],
    sleep_time: int = _DEFAULT_SLEEP_TIME,
    analysis_sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
    *,
    batch_kind: Literal["statistical"],
) -> None:
    pass


@overload
def monitor_batch_job(
    batch_request: BatchStatisticalRequest,
    config: Optional[SHConfig],
    sleep_time: int = _DEFAULT_SLEEP_TIME,
    analysis_sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
    *,
    batch_kind: BatchKind,
) -> None:
    pass


def monitor_batch_job(
    batch_request: BatchRequestSpec,
    config: Optional[SHConfig],
    sleep_time: int = _DEFAULT_SLEEP_TIME,
    analysis_sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
    *,
    batch_kind: BatchKind = "process",
) -> Optional[DefaultDict[BatchTileStatus, List[dict]]]:
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
    batch_kind = _auto_adjust_kind(batch_request, batch_kind)

    batch_request = monitor_batch_analysis(batch_request, config, sleep_time=analysis_sleep_time, batch_kind=batch_kind)
    if batch_request.status is BatchRequestStatus.PROCESSING:
        LOGGER.info("Batch job is running")

    if isinstance(batch_request, BatchRequest):
        return _monitor_batch_process_execution(batch_request, config, sleep_time)
    return _monitor_batch_statistical_execution(batch_request, config, sleep_time)  # type: ignore[func-returns-value]


@overload
def monitor_batch_analysis(
    batch_request: BatchRequestSpec,
    config: Optional[SHConfig],
    sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
    *,
    batch_kind: Literal["process"],
) -> BatchRequest:
    pass


@overload
def monitor_batch_analysis(
    batch_request: BatchRequestSpec,
    config: Optional[SHConfig],
    sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
    *,
    batch_kind: Literal["statistical"],
) -> BatchStatisticalRequest:
    pass


def monitor_batch_analysis(
    batch_request: BatchRequestSpec,
    config: Optional[SHConfig],
    sleep_time: int = _DEFAULT_ANALYSIS_SLEEP_TIME,
    *,
    batch_kind: BatchKind = "process",
) -> Union[BatchRequest, BatchStatisticalRequest]:
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
    batch_kind = _auto_adjust_kind(batch_request, batch_kind)

    batch_client = _get_batch_client(batch_kind, config)
    batch_request = batch_client.get_request(batch_request)  # type: ignore[arg-type]
    while batch_request.status in [BatchRequestStatus.CREATED, BatchRequestStatus.ANALYSING]:
        LOGGER.info("Batch job has a status %s, sleeping for %d seconds", batch_request.status.value, sleep_time)
        time.sleep(sleep_time)
        batch_request = batch_client.get_request(batch_request)  # type: ignore[arg-type]

    batch_request.raise_for_status(status=[BatchRequestStatus.FAILED, BatchRequestStatus.CANCELED])
    return batch_request


def _monitor_batch_process_execution(
    batch_request: BatchRequest, config: Optional[SHConfig], sleep_time: int
) -> DefaultDict[BatchTileStatus, List[dict]]:
    batch_client = SentinelHubBatch(config=config)

    tiles_per_status = _get_batch_tiles_per_status(batch_request, batch_client)
    success_count = len(tiles_per_status[BatchTileStatus.PROCESSED])
    finished_count = success_count + len(tiles_per_status[BatchTileStatus.FAILED])

    with tqdm(total=batch_request.tile_count, initial=finished_count, desc="Progress rate") as progress_bar, tqdm(
        total=finished_count, initial=success_count, desc="Success rate"
    ) as success_bar:
        while finished_count < batch_request.tile_count:
            time.sleep(sleep_time)

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
    return tiles_per_status


def _monitor_batch_statistical_execution(
    batch_request: BatchStatisticalRequest, config: Optional[SHConfig], sleep_time: int
) -> None:
    batch_client = SentinelHubBatchStatistical(config=config)

    def current_completion_percentage() -> float:
        """Fetches the current completion percentage"""
        return batch_client.get_status(batch_request)["completionPercentage"]

    progress = current_completion_percentage()

    with tqdm(total=100, initial=progress, desc="Completion percentage") as progress_bar:
        while progress < 100:
            time.sleep(sleep_time)

            new_progress = current_completion_percentage()
            progress_bar.update(new_progress - progress)
            progress = new_progress


def _get_batch_tiles_per_status(
    batch_request: BatchRequest, batch_client: SentinelHubBatch
) -> DefaultDict[BatchTileStatus, List[dict]]:
    """A helper function that queries information about batch tiles and returns information about tiles, grouped by
    tile status.

    :return: A dictionary mapping a tile status to a list of tile payloads.
    """
    tiles_per_status = defaultdict(list)

    for tile in batch_client.iter_tiles(batch_request):
        status = BatchTileStatus(tile["status"])
        tiles_per_status[status].append(tile)

    return tiles_per_status


def _auto_adjust_kind(batch_request: BatchRequestSpec, batch_kind: BatchKind) -> BatchKind:
    """Fixes any mismatches in batch_request and batch_kind parameters."""
    if isinstance(batch_request, BatchRequest) and batch_kind == "statistical":
        batch_kind = "process"
        warnings.warn(
            "Monitoring was set for statistical requests, but a process request was given. Automatically adjusting"
            " `batch_kind` to `process`."
        )

    if isinstance(batch_request, BatchStatisticalRequest) and batch_kind == "process":
        batch_kind = "statistical"
        warnings.warn(
            "Monitoring was set for process requests, but a statistical request was given. Automatically adjusting"
            " `batch_kind` to `statistical`."
        )
    return batch_kind


@overload
def _get_batch_client(batch_kind: Literal["process"], config: Optional[SHConfig]) -> SentinelHubBatch:
    pass


@overload
def _get_batch_client(batch_kind: Literal["statistical"], config: Optional[SHConfig]) -> SentinelHubBatchStatistical:
    pass


def _get_batch_client(
    batch_kind: BatchKind, config: Optional[SHConfig]
) -> Union[SentinelHubBatch, SentinelHubBatchStatistical]:
    """Initializes the correct batch client"""
    return SentinelHubBatch(config=config) if batch_kind == "process" else SentinelHubBatchStatistical(config=config)

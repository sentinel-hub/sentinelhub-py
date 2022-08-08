"""
A module with tests for batch utilities
"""
import random
from collections import defaultdict
from typing import Dict, List, Tuple

import pytest
from pytest_mock import MockerFixture

from sentinelhub import (
    BatchRequest,
    BatchRequestStatus,
    BatchTileStatus,
    SHConfig,
    monitor_batch_analysis,
    monitor_batch_job,
)


@pytest.mark.parametrize(
    "tile_status_sequence",
    [
        (
            {BatchTileStatus.PENDING: 6},
            {BatchTileStatus.SCHEDULED: 6},
            {BatchTileStatus.PROCESSING: 6},
            {BatchTileStatus.PROCESSED: 6},
        ),
        ({BatchTileStatus.PROCESSED: 2, BatchTileStatus.FAILED: 3},),
    ],
)
@pytest.mark.parametrize("batch_status", [BatchRequestStatus.PROCESSING, BatchRequestStatus.ANALYSIS_DONE])
@pytest.mark.parametrize("config", [SHConfig(), None])
@pytest.mark.parametrize("sleep_time", [60, 1000])
def test_monitor_batch_job(
    tile_status_sequence: Tuple[Dict[BatchTileStatus, int]],
    batch_status: BatchRequestStatus,
    config: SHConfig,
    sleep_time: int,
    mocker: MockerFixture,
) -> None:
    """This test mocks:

    - the method for monitoring batch analysis because that is not a part of this test,
    - requesting info about batch tiles to avoid calling Sentinel Hub service,
    - sleeping time to avoid waiting,
    - logging to ensure logs are being recorded.

    At the end it also checks if all mocks have been called the expected number of times and with expected parameters.
    """
    tiles_sequence = [_tile_status_counts_to_tiles(tile_status_counts) for tile_status_counts in tile_status_sequence]
    tile_count = len(tiles_sequence[0])
    assert all(
        len(tiles) == tile_count for tiles in tiles_sequence
    ), "There should be the same number of tiles in each step. Fix tile_status_sequence parameter of this test."

    batch_request = BatchRequest(
        request_id="mocked-request", process_request={}, tile_count=tile_count, status=batch_status
    )
    monitor_analysis_mock = mocker.patch("sentinelhub.api.batch.utils.monitor_batch_analysis")
    monitor_analysis_mock.return_value = batch_request

    batch_tiles_mock = mocker.patch("sentinelhub.SentinelHubBatch.iter_tiles")
    batch_tiles_mock.side_effect = tiles_sequence

    sleep_mock = mocker.patch("time.sleep")
    logging_mock = mocker.patch("logging.Logger.info")

    results = monitor_batch_job("mocked-request", config=config, sleep_time=sleep_time)

    assert isinstance(results, defaultdict)
    assert set(results) == {BatchTileStatus.PROCESSED, BatchTileStatus.FAILED}
    assert sum(len(tiles) for tiles in results.values()) == tile_count
    for tile_status in [BatchTileStatus.PROCESSED, BatchTileStatus.FAILED]:
        assert len(results[tile_status]) == tile_status_sequence[-1].get(tile_status, 0)

    assert monitor_analysis_mock.call_count == 1

    progress_loop_counts = len(tile_status_sequence) - 1

    assert batch_tiles_mock.call_count == progress_loop_counts + 1
    assert all(call.args == (batch_request,) and call.kwargs == {} for call in batch_tiles_mock.mock_calls)

    assert sleep_mock.call_count == progress_loop_counts
    assert all(call.args == (sleep_time,) and call.kwargs == {} for call in sleep_mock.mock_calls)

    is_processing_logged = batch_status is BatchRequestStatus.PROCESSING
    is_failure_logged = BatchTileStatus.FAILED in tile_status_sequence[-1]
    assert logging_mock.call_count == int(is_processing_logged) + int(is_failure_logged)


def _tile_status_counts_to_tiles(tile_status_counts: Dict[BatchTileStatus, int]) -> List[Dict[str, str]]:
    """From the info about how many tiles should have certain status it generates a list of tile payloads with these
    statuses.

    Each payload should be approximately what Sentinel Hub service returns but because we don't need all parameters we
    just return status. At the end we randomly shuffle the list just to make it more general.
    """
    tiles: List[Dict[str, str]] = []
    for tile_status, count in tile_status_counts.items():
        for _ in range(count):
            tiles.append({"status": tile_status.value})

    random.shuffle(tiles)
    return tiles


def test_monitor_batch_job_sleep_time_error() -> None:
    with pytest.raises(ValueError):
        monitor_batch_job("x", sleep_time=59)

    with pytest.raises(ValueError):
        monitor_batch_job("x", analysis_sleep_time=4)


@pytest.mark.parametrize(
    "status_sequence",
    [
        (BatchRequestStatus.CREATED, BatchRequestStatus.ANALYSING, BatchRequestStatus.ANALYSIS_DONE),
        (BatchRequestStatus.DONE,),
        (BatchRequestStatus.ANALYSING, BatchRequestStatus.CANCELED),
        (BatchRequestStatus.FAILED,),
    ],
)
@pytest.mark.parametrize("config", [SHConfig(), None])
@pytest.mark.parametrize("sleep_time", [5, 1000])
def test_monitor_batch_analysis(
    status_sequence: Tuple[BatchRequestStatus, ...], config: SHConfig, sleep_time: int, mocker: MockerFixture
) -> None:
    """This test mocks:

    - a method of Batch API interface to avoid calling Sentinel Hub service,
    - sleeping time to avoid waiting,
    - logging to ensure logs are being recorded.

    At the end it also checks if all mocks have been called the expected number of times and with expected parameters.
    """
    batch_requests = [
        BatchRequest(request_id="mocked-request", process_request={}, tile_count=42, status=status)
        for status in status_sequence
    ]

    batch_mock = mocker.patch("sentinelhub.SentinelHubBatch.get_request")
    batch_mock.side_effect = batch_requests
    sleep_mock = mocker.patch("time.sleep")
    logging_mock = mocker.patch("logging.Logger.info")

    if status_sequence[-1] in [BatchRequestStatus.CANCELED, BatchRequestStatus.FAILED]:
        with pytest.raises(RuntimeError):
            monitor_batch_analysis("mocked-request", config=config, sleep_time=sleep_time)
    else:
        result = monitor_batch_analysis("mocked-request", config=config, sleep_time=sleep_time)
        assert result is batch_requests[-1]

    sleep_loop_counts = len(status_sequence) - 1

    assert batch_mock.call_count == sleep_loop_counts + 1
    assert batch_mock.mock_calls[0].args == ("mocked-request",)
    assert all(
        call.args == (request,) and call.kwargs == {}
        for call, request in zip(batch_mock.mock_calls[1:], batch_requests)
    )

    assert sleep_mock.call_count == sleep_loop_counts
    assert all(call.args == (sleep_time,) and call.kwargs == {} for call in sleep_mock.mock_calls)

    assert logging_mock.call_count == sleep_loop_counts


def test_monitor_batch_analysis_sleep_time_error() -> None:
    with pytest.raises(ValueError):
        monitor_batch_analysis("x", sleep_time=4)

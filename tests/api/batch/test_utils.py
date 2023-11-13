"""
A module with tests for batch utilities

IMPORTANT: These tests intentionally break the "black box principle" of testing because these are not ordinary functions
we are testing. It is not enough to just verify correctness of outputs. We also have to verify the procedure itself:
how many interactions with Sentinel Hub service they make, how much time they sleep, and how many logs they report.
Because of that the tests are very strict. If you break them make sure to understand what is happening before either
changing the code or the tests.
"""

from __future__ import annotations

import random
from collections import defaultdict
from typing import Callable, Sequence

import pytest
from pytest_mock import MockerFixture

from sentinelhub import (
    BatchRequest,
    BatchRequestStatus,
    BatchStatisticalRequest,
    BatchTileStatus,
    SHConfig,
    monitor_batch_analysis,
    monitor_batch_job,
    monitor_batch_statistical_analysis,
    monitor_batch_statistical_job,
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
def test_monitor_batch_process_job(
    tile_status_sequence: tuple[dict[BatchTileStatus, int], ...],
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


def _tile_status_counts_to_tiles(tile_status_counts: dict[BatchTileStatus, int]) -> list[dict[str, str]]:
    """From the info about how many tiles should have certain status it generates a list of tile payloads with these
    statuses.

    Each payload should be approximately what Sentinel Hub service returns but because we don't need all parameters we
    just return status. At the end we randomly shuffle the list just to make it more general.
    """
    tiles: list[dict[str, str]] = [
        {"status": tile_status.value} for tile_status, count in tile_status_counts.items() for _ in range(count)
    ]

    random.shuffle(tiles)
    return tiles


@pytest.mark.parametrize("batch_status", [BatchRequestStatus.PROCESSING, BatchRequestStatus.ANALYSIS_DONE])
@pytest.mark.parametrize(
    "progress_sequence",
    [
        (0, 10, 30.5, 70, 90, 99, 100),
        (50.712, 80, 100),
        (100,),
    ],
)
@pytest.mark.parametrize("config", [SHConfig(), None])
@pytest.mark.parametrize("sleep_time", [15, 1000])
def test_monitor_batch_statistical_job(
    batch_status: BatchRequestStatus,
    progress_sequence: Sequence[float],
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

    batch_request = BatchStatisticalRequest(
        request_id="mocked-request", request={}, status=batch_status, completion_percentage=0
    )
    monitor_analysis_mock = mocker.patch("sentinelhub.api.batch.utils.monitor_batch_statistical_analysis")
    monitor_analysis_mock.return_value = batch_request
    get_status_mock = mocker.patch("sentinelhub.SentinelHubBatchStatistical.get_status")
    get_status_mock.side_effect = ({"completionPercentage": x} for x in progress_sequence)

    sleep_mock = mocker.patch("time.sleep")
    logging_mock = mocker.patch("logging.Logger.info")

    results = monitor_batch_statistical_job("mocked-request", config=config, sleep_time=sleep_time)

    assert isinstance(results, dict)
    assert results["completionPercentage"] == 100

    assert monitor_analysis_mock.call_count == 1

    assert sleep_mock.call_count == len(progress_sequence) - 1
    assert get_status_mock.call_count == len(progress_sequence)
    assert all(call.args == (sleep_time,) and call.kwargs == {} for call in sleep_mock.mock_calls)

    is_processing_logged = batch_status is BatchRequestStatus.PROCESSING
    assert logging_mock.call_count == int(is_processing_logged)


@pytest.mark.parametrize(
    ("monitor_function", "sleep_time"), [(monitor_batch_job, 59), (monitor_batch_statistical_job, 14)]
)
def test_monitor_batch_job_sleep_time_error(monitor_function: Callable, sleep_time: int) -> None:
    with pytest.raises(ValueError):
        monitor_function("x", sleep_time=sleep_time)

    with pytest.raises(ValueError):
        monitor_function("x", analysis_sleep_time=4)


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
    status_sequence: tuple[BatchRequestStatus, ...], config: SHConfig, sleep_time: int, mocker: MockerFixture
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
def test_monitor_batch_statistical_analysis(
    status_sequence: tuple[BatchRequestStatus, ...], config: SHConfig, sleep_time: int, mocker: MockerFixture
) -> None:
    """This test mocks:

    - a method of Batch API interface to avoid calling Sentinel Hub service,
    - sleeping time to avoid waiting,
    - logging to ensure logs are being recorded.

    At the end it also checks if all mocks have been called the expected number of times and with expected parameters.
    """
    request_statuses = [{"status": status} for status in status_sequence]
    final_request = BatchStatisticalRequest(
        "mocked-request", completion_percentage=0, request={}, status=status_sequence[-1]
    )

    batch_mock = mocker.patch("sentinelhub.SentinelHubBatchStatistical.get_request")
    batch_mock.return_value = final_request
    status_mock = mocker.patch("sentinelhub.SentinelHubBatchStatistical.get_status")
    status_mock.side_effect = request_statuses
    sleep_mock = mocker.patch("time.sleep")
    logging_mock = mocker.patch("logging.Logger.info")

    if status_sequence[-1] in [BatchRequestStatus.CANCELED, BatchRequestStatus.FAILED]:
        with pytest.raises(RuntimeError):
            monitor_batch_statistical_analysis("mocked-request", config=config, sleep_time=sleep_time)
    else:
        result = monitor_batch_statistical_analysis("mocked-request", config=config, sleep_time=sleep_time)
        assert result is final_request
        assert batch_mock.call_count == 1
        assert batch_mock.mock_calls[0].args == ("mocked-request",)

    assert sleep_mock.call_count == len(status_sequence) - 1
    assert all(call.args == (sleep_time,) and call.kwargs == {} for call in sleep_mock.mock_calls)

    assert status_mock.call_count == len(status_sequence)
    assert all(call.args == ("mocked-request",) and call.kwargs == {} for call in status_mock.mock_calls)

    assert logging_mock.call_count == len(status_sequence) - 1


@pytest.mark.parametrize("monitor_function", [monitor_batch_analysis, monitor_batch_statistical_analysis])
def test_monitor_batch_analysis_sleep_time_error(monitor_function: Callable) -> None:
    with pytest.raises(ValueError):
        monitor_function("x", sleep_time=4)

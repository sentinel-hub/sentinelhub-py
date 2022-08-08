"""
A module with tests for batch utilities
"""
from typing import Tuple

import pytest
from pytest_mock import MockerFixture

from sentinelhub import BatchRequest, BatchRequestStatus, SHConfig, monitor_batch_analysis, monitor_batch_job


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
    - time sleeping avoid waiting,
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

    assert batch_mock.call_count == len(status_sequence)
    assert batch_mock.mock_calls[0].args == ("mocked-request",)
    assert all(
        call.args == (request,) and call.kwargs == {}
        for call, request in zip(batch_mock.mock_calls[1:], batch_requests)
    )

    assert sleep_mock.call_count == len(status_sequence) - 1
    assert all(call.args == (sleep_time,) and call.kwargs == {} for call in sleep_mock.mock_calls)

    assert logging_mock.call_count == len(status_sequence) - 1


def test_monitor_batch_analysis_sleep_time_error() -> None:
    with pytest.raises(ValueError):
        monitor_batch_analysis("x", sleep_time=4)

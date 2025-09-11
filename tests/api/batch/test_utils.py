"""
A module with tests for batch utilities

IMPORTANT: These tests intentionally break the "black box principle" of testing because these are not ordinary functions
we are testing. It is not enough to just verify correctness of outputs. We also have to verify the procedure itself:
how many interactions with Sentinel Hub service they make, how much time they sleep, and how many logs they report.
Because of that the tests are very strict. If you break them make sure to understand what is happening before either
changing the code or the tests.
"""

from __future__ import annotations

from typing import Sequence

import pytest
from pytest_mock import MockerFixture

from sentinelhub import (
    BatchProcessClient,
    BatchProcessRequest,
    BatchRequestStatus,
    BatchStatisticalRequest,
    SHConfig,
    monitor_batch_process_analysis,
    monitor_batch_process_job,
    monitor_batch_statistical_analysis,
    monitor_batch_statistical_job,
)


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
    "status_sequence",
    [
        (BatchRequestStatus.CREATED, BatchRequestStatus.ANALYSING, BatchRequestStatus.ANALYSIS_DONE),
        (BatchRequestStatus.DONE,),
        (BatchRequestStatus.ANALYSING, BatchRequestStatus.STOPPED),
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

    if status_sequence[-1] in [BatchRequestStatus.STOPPED, BatchRequestStatus.FAILED]:
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


def test_monitor_batch_analysis_sleep_time_error() -> None:
    with pytest.raises(ValueError):
        monitor_batch_statistical_analysis("x", sleep_time=4)


@pytest.mark.parametrize("batch_status", [BatchRequestStatus.PROCESSING, BatchRequestStatus.ANALYSIS_DONE])
@pytest.mark.parametrize(
    "progress_sequence",
    [
        (0, 10, 30.5, 70, 90, 99, 100, 100, 100, 100),  # only the last one is DONE
        (50.712, 80, 100),
        (100,),
    ],
)
@pytest.mark.parametrize("sleep_time", [75, 1000])
def test_monitor_batch_process_job(
    batch_status: BatchRequestStatus, progress_sequence: Sequence[float], sleep_time: int, mocker: MockerFixture
) -> None:
    """This test mocks:

    - the method for monitoring batch analysis because that is not a part of this test,
    - requesting info about batch tiles to avoid calling Sentinel Hub service,
    - sleeping time to avoid waiting,
    - logging to ensure logs are being recorded.

    At the end it also checks if all mocks have been called the expected number of times and with expected parameters.
    """

    batch_request = BatchProcessRequest(
        request_id="mocked-request", domain_account_id="test", request={}, status=batch_status, completion_percentage=0
    )

    # skip analysis
    monitor_analysis_mock = mocker.patch("sentinelhub.api.batch.utils.monitor_batch_process_analysis")
    monitor_analysis_mock.return_value = batch_request

    get_status_mock = mocker.patch("sentinelhub.BatchProcessClient.get_request")
    returned_requests = [
        BatchProcessRequest(
            request_id="mocked-request",
            domain_account_id="test",
            completion_percentage=x,
            request={},
            status=batch_status,
        )
        for x in progress_sequence
    ]
    returned_requests[-1].status = BatchRequestStatus.DONE  # last one must be done
    get_status_mock.side_effect = returned_requests

    sleep_mock = mocker.patch("time.sleep")

    client = BatchProcessClient()
    result = monitor_batch_process_job(batch_request, client, sleep_time=sleep_time)

    assert isinstance(result, BatchProcessRequest)
    assert result.completion_percentage == 100

    assert monitor_analysis_mock.call_count == 1

    assert sleep_mock.call_count == len(progress_sequence)
    assert get_status_mock.call_count == len(progress_sequence)
    assert all(call.args == (sleep_time,) and call.kwargs == {} for call in sleep_mock.mock_calls)


@pytest.mark.parametrize(
    "status_sequence",
    [
        (BatchRequestStatus.CREATED, BatchRequestStatus.ANALYSING, BatchRequestStatus.ANALYSIS_DONE),
        (BatchRequestStatus.DONE,),
        (BatchRequestStatus.ANALYSING, BatchRequestStatus.STOPPED),
        (BatchRequestStatus.FAILED,),
    ],
)
@pytest.mark.parametrize("sleep_time", [5, 1000])
def test_monitor_batch_process_analysis(
    status_sequence: tuple[BatchRequestStatus, ...], sleep_time: int, mocker: MockerFixture
) -> None:
    """This test mocks:

    - a method of Batch API interface to avoid calling Sentinel Hub service,
    - sleeping time to avoid waiting,
    - logging to ensure logs are being recorded.

    At the end it also checks if all mocks have been called the expected number of times and with expected parameters.
    """
    requests = [
        BatchProcessRequest("mocked-request", domain_account_id="test", request={}, status=status)
        for status in status_sequence
    ]

    client = BatchProcessClient()

    batch_mock = mocker.patch("sentinelhub.BatchProcessClient.get_request")
    batch_mock.side_effect = requests
    sleep_mock = mocker.patch("time.sleep")
    logging_mock = mocker.patch("logging.Logger.info")

    if status_sequence[-1] in [BatchRequestStatus.STOPPED, BatchRequestStatus.FAILED]:
        with pytest.raises(RuntimeError):
            monitor_batch_process_analysis("mocked-request", client, sleep_time=sleep_time)
    else:
        result = monitor_batch_process_analysis("mocked-request", client, sleep_time=sleep_time)
        assert result is requests[-1]
        assert batch_mock.call_count == len(requests)
        assert batch_mock.mock_calls[0].args == ("mocked-request",)

    assert sleep_mock.call_count == len(status_sequence) - 1
    assert all(call.args == (sleep_time,) and call.kwargs == {} for call in sleep_mock.mock_calls)

    assert logging_mock.call_count == len(status_sequence) - 1

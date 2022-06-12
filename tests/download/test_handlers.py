"""
Unit tests for download handlers
"""
import pytest
from requests import Response
from requests.exceptions import ChunkedEncodingError, ConnectionError, HTTPError, InvalidJSONError, Timeout

from sentinelhub import DownloadFailedException, DownloadRequest, SHConfig
from sentinelhub.download.handlers import fail_missing_file, fail_user_errors, retry_temporary_errors


class DummyClient:
    """A minimal object required by handlers and that can remember how many times it was called."""

    def __init__(self):
        self.config = SHConfig()
        self.config.max_download_attempts = 8
        self.config.download_sleep_time = 0

        self.count = 0

    def increase_count(self) -> None:
        self.count += 1


def _build_http_error(status_code: int) -> HTTPError:
    """Creates an instance of `HTTPError` with a given status code"""
    response = Response()
    response.status_code = status_code
    return HTTPError(response=response)


@pytest.mark.parametrize(
    "exception, is_expected_handling",
    [
        (ConnectionError("No connection"), True),
        (Timeout(), True),
        (ChunkedEncodingError(), True),
        (_build_http_error(500), True),
        (_build_http_error(503), True),
        (_build_http_error(404), False),
        (_build_http_error(429), False),
        (InvalidJSONError(), False),
        (ValueError(), False),
    ],
)
def test_retry_temporary_errors(exception: Exception, is_expected_handling: bool) -> None:
    dummy_client = DummyClient()

    @retry_temporary_errors
    def fail_process(client: DummyClient, _: DownloadRequest) -> None:
        client.increase_count()
        raise exception

    expected_exception_class = DownloadFailedException if is_expected_handling else exception.__class__
    with pytest.raises(expected_exception_class):
        fail_process(dummy_client, DownloadRequest())

    if is_expected_handling:
        assert dummy_client.count == dummy_client.config.max_download_attempts


@pytest.mark.parametrize(
    "exception, is_expected_handling",
    [
        (_build_http_error(400), True),
        (_build_http_error(404), True),
        (_build_http_error(429), False),
        (_build_http_error(451), True),
        (_build_http_error(500), False),
        (_build_http_error(503), False),
        (ConnectionError(), False),
        (Timeout(), False),
        (ChunkedEncodingError(), False),
        (InvalidJSONError(), False),
        (ValueError(), False),
    ],
)
def test_fail_user_errors(exception: Exception, is_expected_handling: bool) -> None:
    dummy_client = DummyClient()

    @fail_user_errors
    def fail_process(client: DummyClient, _: DownloadRequest) -> None:
        client.increase_count()
        raise exception

    expected_exception_class = DownloadFailedException if is_expected_handling else exception.__class__
    with pytest.raises(expected_exception_class):
        fail_process(dummy_client, DownloadRequest())

    if is_expected_handling:
        assert dummy_client.count == 1


@pytest.mark.parametrize(
    "exception, is_expected_handling",
    [
        (_build_http_error(400), False),
        (_build_http_error(404), True),
        (_build_http_error(429), False),
        (_build_http_error(500), False),
        (_build_http_error(503), False),
        (ConnectionError(), False),
        (Timeout(), False),
        (ChunkedEncodingError(), False),
        (InvalidJSONError(), False),
        (ValueError(), False),
    ],
)
def test_fail_missing_file(exception: Exception, is_expected_handling: bool) -> None:
    dummy_client = DummyClient()

    @fail_missing_file
    def fail_process(client: DummyClient, _: DownloadRequest) -> None:
        client.increase_count()
        raise exception

    expected_exception_class = DownloadFailedException if is_expected_handling else exception.__class__
    with pytest.raises(expected_exception_class):
        fail_process(dummy_client, DownloadRequest())

    if is_expected_handling:
        assert dummy_client.count == 1

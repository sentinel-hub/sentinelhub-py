import time
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, Optional, Type

import pytest
from oauthlib.oauth2.rfc6749.errors import CustomOAuth2Error
from requests_mock import Mocker

from sentinelhub import SentinelHubSession, SHConfig, __version__
from sentinelhub.download import SessionSharing, SessionSharingThread, collect_shared_session
from sentinelhub.exceptions import DownloadFailedException, SHUserWarning
from sentinelhub.types import JsonDict


@pytest.fixture(name="fake_config")
def fake_config_fixture() -> SHConfig:
    config = SHConfig()
    config.sh_client_id = "sh-py-test"
    config.sh_client_secret = "sh-py-test"
    return config


@pytest.fixture(name="fake_token")
def fake_token_fixture() -> JsonDict:
    return {"access_token": "x", "expires_in": 1000, "expires_at": time.time() + 1000}


@pytest.mark.sh_integration()
def test_session(session: SentinelHubSession) -> None:
    token = session.token
    headers = session.session_headers

    for item in [token, headers]:
        assert isinstance(item, dict)

    for key in ["access_token", "expires_in", "expires_at"]:
        assert key in token

    same_token = session.token
    assert token["access_token"] == same_token["access_token"], "The token has been refreshed"

    token["expires_at"] = 0
    new_token = session.token
    assert token["access_token"] != new_token["access_token"], "The token has not been refreshed"


@pytest.mark.sh_integration()
def test_token_info(session: SentinelHubSession) -> None:
    info = session.info()

    for key in ["sub", "aud", "jti", "exp", "name", "email", "sid", "org", "did", "aid", "d"]:
        assert key in info


def test_session_content_and_headers(fake_config: SHConfig, fake_token: Dict[str, Any], requests_mock: Mocker) -> None:
    """Make sure correct content and headers are passed to the service."""
    requests_mock.post(url="/oauth/token", response_list=[{"json": fake_token}])
    call_time = time.time()
    token = SentinelHubSession(config=fake_config).token
    # "expires_at" is derived from "expires_in"  and not read from the response field "expires_at"
    # this can cause a mismatch, but tokens are refreshed 120s before "expires_at" so a few seconds is fine
    assert token["expires_at"] == pytest.approx(call_time + fake_token["expires_in"], 1.0)
    token["expires_at"] = fake_token["expires_at"]
    assert token == fake_token

    assert len(requests_mock.request_history) == 1
    mocked_request = requests_mock.request_history[0]

    assert mocked_request.url == fake_config.sh_token_url
    assert mocked_request.headers["User-Agent"] == f"sentinelhub-py/v{__version__}"
    assert mocked_request.headers["Content-Type"] == "application/x-www-form-urlencoded"


def test_session_with_missing_credentials(fake_token: JsonDict) -> None:
    config_without_credentials = SHConfig(use_defaults=True)

    with pytest.raises(ValueError):
        SentinelHubSession(config=config_without_credentials)

    with pytest.raises(ValueError):
        SentinelHubSession(config=config_without_credentials, _token=fake_token)

    # This succeeds because it receives and existing token and refreshing is deactivated
    session = SentinelHubSession(config=config_without_credentials, refresh_before_expiry=None, _token=fake_token)
    assert session.token == fake_token


def test_from_token(fake_token: JsonDict) -> None:
    session = SentinelHubSession.from_token(fake_token)
    assert session.token == fake_token
    assert session.refresh_before_expiry is None

    fake_token["expires_at"] -= fake_token["expires_in"]
    session = SentinelHubSession.from_token(fake_token)
    with pytest.warns(SHUserWarning):
        expired_token = session.token
    assert expired_token == fake_token


@pytest.mark.sh_integration()
def test_refreshing_procedure(fake_token: JsonDict, fake_config: SHConfig) -> None:
    fake_token["expires_at"] -= 500

    for expiry in [None, 400]:
        session = SentinelHubSession(config=fake_config, refresh_before_expiry=expiry, _token=fake_token)
        assert session.token == fake_token

    session = SentinelHubSession(config=fake_config, refresh_before_expiry=500, _token=fake_token)
    with pytest.raises(CustomOAuth2Error):
        _ = session.token


@pytest.mark.parametrize("status_code", [400, 404])
@pytest.mark.parametrize(
    ("response_payload", "expected_exception"),
    [
        ({"error": "Mocked error message", "access_token": "xxx"}, CustomOAuth2Error),
        ({"access_token": "xxx"}, DownloadFailedException),
        (None, DownloadFailedException),
    ],
)
def test_oauth_compliance_hook_4xx(
    requests_mock: Mocker,
    status_code: int,
    response_payload: Optional[JsonDict],
    expected_exception: Type[Exception],
    fake_config: SHConfig,
) -> None:
    requests_mock.post(
        "https://services.sentinel-hub.com/oauth/token",
        json=response_payload,
        status_code=status_code,
    )

    with pytest.raises(expected_exception):
        SentinelHubSession(config=fake_config)
    assert len(requests_mock.request_history) == 1


@pytest.mark.parametrize("status_code", [500, 503])
@pytest.mark.parametrize(
    "response_payload",
    [
        ({"error": "Mocked error message", "access_token": "xxx"},),
        ({"access_token": "xxx"},),
        (None,),
    ],
)
def test_oauth_compliance_hook_5xx(
    requests_mock: Mocker, status_code: int, response_payload: Optional[JsonDict], fake_config: SHConfig
) -> None:
    requests_mock.post(
        "https://services.sentinel-hub.com/oauth/token",
        json=response_payload,
        status_code=status_code,
    )

    fake_config.max_download_attempts = 10
    fake_config.download_sleep_time = 0

    with pytest.raises(DownloadFailedException):
        SentinelHubSession(config=fake_config)
    assert len(requests_mock.request_history) == fake_config.max_download_attempts


@pytest.mark.parametrize("memory_name", [None, "test-name"])
def test_session_sharing_single_process(
    fake_token: JsonDict, fake_config: SHConfig, memory_name: Optional[str]
) -> None:
    session = SentinelHubSession(config=fake_config, refresh_before_expiry=0, _token=fake_token)

    kwargs = {} if memory_name is None else {"memory_name": memory_name}
    thread = SessionSharingThread(session, name="thread name", **kwargs)
    assert thread.name == "thread name"

    thread.start()

    try:
        collected_session = collect_shared_session(**kwargs)
        assert collected_session.token == fake_token
    finally:
        thread.join()


@pytest.mark.parametrize("memory_name", [None, "test-name"])
def test_session_sharing_multiprocess(fake_token: JsonDict, fake_config: SHConfig, memory_name: Optional[str]) -> None:
    session = SentinelHubSession(config=fake_config, refresh_before_expiry=0, _token=fake_token)

    kwargs = {} if memory_name is None else {"memory_name": memory_name}
    thread = SessionSharingThread(session, **kwargs)
    thread.start()

    try:
        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(collect_shared_session, **kwargs) for _ in range(10)]
            collected_sessions = [future.result() for future in futures]

        assert all(collected_session.token == fake_token for collected_session in collected_sessions)
    finally:
        thread.join()


@pytest.mark.parametrize("memory_name", [None, "test-name"])
def test_session_sharing_object(fake_token: JsonDict, fake_config: SHConfig, memory_name: Optional[str]) -> None:
    session = SentinelHubSession(config=fake_config, refresh_before_expiry=0, _token=fake_token)

    kwargs = {} if memory_name is None else {"memory_name": memory_name}
    manager = SessionSharing(session, name="thread name", **kwargs)

    with pytest.raises(FileNotFoundError):
        collect_shared_session(**kwargs)

    with manager:
        collected_session = collect_shared_session(**kwargs)
        assert collected_session.token == fake_token

    with pytest.raises(FileNotFoundError):
        collect_shared_session(**kwargs)


def test_handling_of_unclosed_memory(fake_token: JsonDict, fake_config: SHConfig) -> None:
    session = SentinelHubSession(config=fake_config, refresh_before_expiry=0, _token=fake_token)

    thread1 = SessionSharingThread(session)
    thread1.start()

    thread2 = SessionSharingThread(session)
    with pytest.warns(SHUserWarning):
        thread2.start()

    thread1.join()
    thread2.join()

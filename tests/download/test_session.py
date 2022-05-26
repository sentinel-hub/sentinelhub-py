import time
from concurrent.futures import ProcessPoolExecutor

import pytest
from oauthlib.oauth2.rfc6749.errors import CustomOAuth2Error

from sentinelhub import SentinelHubSession, SHConfig
from sentinelhub.download import SessionSharingThread, collect_shared_session
from sentinelhub.exceptions import SHUserWarning


@pytest.fixture(name="fake_config")
def fake_config_fixture():
    config = SHConfig()
    config.sh_client_id = "sh-py-test"
    config.sh_client_secret = "sh-py-test"
    return config


@pytest.fixture(name="fake_token")
def fake_token_fixture():
    return {"access_token": "x", "expires_in": 1000, "expires_at": time.time() + 1000}


@pytest.mark.sh_integration
def test_session(session):
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


@pytest.mark.sh_integration
def test_token_info(session):
    info = session.info()

    for key in ["sub", "aud", "jti", "exp", "name", "email", "sid", "org", "did", "aid", "d"]:
        assert key in info


def test_session_with_missing_credentials(fake_token):
    config_without_credentials = SHConfig(use_defaults=True)

    with pytest.raises(ValueError):
        SentinelHubSession(config=config_without_credentials)

    with pytest.raises(ValueError):
        SentinelHubSession(config=config_without_credentials, _token=fake_token)

    # This succeeds because it receives and existing token and refreshing is deactivated
    session = SentinelHubSession(config=config_without_credentials, refresh_before_expiry=None, _token=fake_token)
    assert session.token == fake_token


def test_from_token(fake_token):
    session = SentinelHubSession.from_token(fake_token)
    assert session.token == fake_token
    assert session.refresh_before_expiry is None

    fake_token["expires_at"] -= 1000
    session = SentinelHubSession.from_token(fake_token)
    with pytest.warns(SHUserWarning):
        expired_token = session.token
    assert expired_token == fake_token


@pytest.mark.sh_integration
def test_refreshing_procedure(fake_token, fake_config):
    fake_token["expires_at"] -= 500

    for expiry in [None, 400]:
        session = SentinelHubSession(config=fake_config, refresh_before_expiry=expiry, _token=fake_token)
        assert session.token == fake_token

    session = SentinelHubSession(config=fake_config, refresh_before_expiry=500, _token=fake_token)
    with pytest.raises(CustomOAuth2Error):
        _ = session.token


@pytest.mark.parametrize("memory_name", [None, "test-name"])
def test_session_sharing_single_process(fake_token, fake_config, memory_name):
    session = SentinelHubSession(config=fake_config, refresh_before_expiry=0, _token=fake_token)

    kwargs = {} if memory_name is None else {"memory_name": memory_name}
    thread = SessionSharingThread(session, name="thread name", **kwargs)
    assert thread.name == "thread name"

    thread.start()

    try:
        collected_session = collect_shared_session(**kwargs)
        assert collected_session.token == fake_token
    finally:
        thread.stop()


@pytest.mark.parametrize("memory_name", [None, "test-name"])
def test_session_sharing_multiprocess(fake_token, fake_config, memory_name):
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
        thread.stop()

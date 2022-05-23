import time

import pytest
from oauthlib.oauth2.rfc6749.errors import CustomOAuth2Error

from sentinelhub import SentinelHubSession, SHConfig
from sentinelhub.exceptions import SHUserWarning


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
    config_without_credentials = SHConfig()
    config_without_credentials.reset()

    with pytest.raises(ValueError):
        SentinelHubSession(config=config_without_credentials)

    with pytest.raises(ValueError):
        SentinelHubSession(config=config_without_credentials, _token=fake_token)

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
def test_refreshing_procedure(fake_token):
    config = SHConfig()
    config.sh_client_id = "sh-py-test"
    config.sh_client_secret = "sh-py-test"

    fake_token["expires_at"] -= 500

    for expiry in [None, 400]:
        session = SentinelHubSession(config=config, refresh_before_expiry=expiry, _token=fake_token)
        assert session.token == fake_token

    session = SentinelHubSession(config=config, refresh_before_expiry=500, _token=fake_token)
    with pytest.raises(CustomOAuth2Error):
        _ = session.token

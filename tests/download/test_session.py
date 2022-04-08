import pytest

from sentinelhub import SentinelHubSession


@pytest.fixture(name="session", scope="module")
def session_fixture():
    session = SentinelHubSession()
    return session


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

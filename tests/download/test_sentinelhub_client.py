import pytest

from sentinelhub import SentinelHubDownloadClient, SentinelHubStatisticalDownloadClient, SHConfig


@pytest.mark.sh_integration
def test_client_with_fixed_session(session):
    blank_config = SHConfig(use_defaults=True)
    client = SentinelHubDownloadClient(session=session, config=blank_config)

    used_session = client.get_session()
    assert used_session is session

    info = client.get_json("https://services.sentinel-hub.com/oauth/tokeninfo", use_session=True)
    assert info

    with pytest.raises(ValueError):
        SentinelHubDownloadClient(session=blank_config, config=blank_config)


@pytest.mark.sh_integration
@pytest.mark.parametrize("client_object", [SentinelHubDownloadClient, SentinelHubDownloadClient()])
def test_session_caching_and_clearing(client_object, session):
    client_object.clear_cache()
    assert SentinelHubDownloadClient._CACHED_SESSIONS == {}

    client_object.cache_session(session)
    assert len(SentinelHubDownloadClient._CACHED_SESSIONS) == 1
    assert list(SentinelHubDownloadClient._CACHED_SESSIONS.values()) == [session]

    client_object.clear_cache()
    assert SentinelHubDownloadClient._CACHED_SESSIONS == {}


@pytest.mark.sh_integration
def test_session_caching_on_subclass(session):
    statistical_client = SentinelHubStatisticalDownloadClient()
    statistical_client.cache_session(session)

    assert len(SentinelHubDownloadClient._CACHED_SESSIONS) == 1
    assert list(SentinelHubDownloadClient._CACHED_SESSIONS.values()) == [session]

    client = SentinelHubDownloadClient()
    used_session = client.get_session()
    assert used_session is session

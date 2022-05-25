import pytest

from sentinelhub import SentinelHubDownloadClient, SentinelHubSession, SentinelHubStatisticalDownloadClient, SHConfig

FAST_SH_ENDPOINT = "https://services.sentinel-hub.com/api/v1/catalog/collections"


@pytest.mark.sh_integration
def test_client_with_fixed_session(session):
    blank_config = SHConfig(use_defaults=True)
    client = SentinelHubDownloadClient(session=session, config=blank_config)

    obtained_session = client.get_session()
    assert obtained_session is session

    info = client.get_json(FAST_SH_ENDPOINT, use_session=True)
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


def test_double_session_caching(session):
    another_session = SentinelHubSession()

    client = SentinelHubDownloadClient()
    client.cache_session(session)
    assert client.get_session() is session

    client.cache_session(another_session)
    assert client.get_session() is another_session

    client_with_fixed_session = SentinelHubDownloadClient(session=session)
    assert client_with_fixed_session.get_session() is session

    client_with_fixed_session.cache_session(another_session)
    assert client_with_fixed_session.get_session() is session


@pytest.mark.sh_integration
def test_session_caching_on_subclass(session):
    statistical_client = SentinelHubStatisticalDownloadClient()
    statistical_client.cache_session(session)

    assert len(SentinelHubDownloadClient._CACHED_SESSIONS) == 1
    assert list(SentinelHubDownloadClient._CACHED_SESSIONS.values()) == [session]

    client = SentinelHubDownloadClient()
    obtained_session = client.get_session()
    assert obtained_session is session


@pytest.mark.sh_integration
def test_universal_session_caching(session):
    SentinelHubDownloadClient.clear_cache()

    config_without_credentials = SHConfig(use_defaults=True)
    client = SentinelHubDownloadClient(config=config_without_credentials)

    with pytest.raises(ValueError):
        client.get_session()

    SentinelHubDownloadClient.cache_session(session, universal=False)
    with pytest.raises(ValueError):
        client.get_session()

    SentinelHubDownloadClient.cache_session(session, universal=True)
    cached_session = client.get_session()
    assert cached_session is session

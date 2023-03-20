"""
Unit tests for config.py module
"""
import json
import os
import shutil
from typing import Any, Generator

import pytest

from sentinelhub import SHConfig


@pytest.fixture(autouse=True, scope="module")
def mask_and_restore_config_fixture() -> Generator[None, None, None]:
    """A fixture that makes sure original config is restored after tests are executed. It restores the config even if
    a test has failed.
    """
    config_path = SHConfig.get_config_location()
    cache_path = config_path.replace(".json", "_test_cache.json")
    shutil.move(config_path, cache_path)

    # Create a mock config
    config = SHConfig(use_defaults=True)
    config.geopedia_wms_url = "zero-drama-llama.com"
    config.download_timeout_seconds = 100
    config.max_download_attempts = 42
    config.save()

    yield

    os.remove(config_path)
    shutil.move(cache_path, config_path)


@pytest.fixture(name="restore_config_file")
def restore_config_file_fixture() -> Generator[None, None, None]:
    """A fixture that ensures the config file is reset after the test."""
    config = SHConfig()
    yield
    config.save()


@pytest.fixture(name="test_config")
def test_config_fixture() -> SHConfig:
    config = SHConfig(use_defaults=True)
    config.instance_id = "fake_instance_id"
    config.sh_client_id = "tester"
    config.sh_client_secret = "1_l1k3-p1n34ppl3*0n%p1224"
    return config


@pytest.mark.dependency()
def test_fake_config_during_tests() -> None:
    config = SHConfig()
    credentials_removed = all(getattr(config, field) == "" for field in config.CREDENTIALS)
    assert credentials_removed, "Credentials not properly removed for testing. Aborting tests."


@pytest.mark.dependency(depends=["test_fake_config_during_tests"])
def test_config_file() -> None:
    config = SHConfig()
    config_file = config.get_config_location()
    assert os.path.isfile(config_file), f"Config file does not exist: {os.path.abspath(config_file)}"

    with open(config_file, "r") as file_handle:
        config_dict = json.load(file_handle)

    for param, value in config_dict.items():
        if param in config.CREDENTIALS:
            continue

        if isinstance(value, str):
            value = value.rstrip("/")

        assert getattr(config, param) == value, f"Parameter {param} does not match it's equivalent in the config.json."


@pytest.mark.dependency(depends=["test_fake_config_during_tests"])
def test_set_and_reset_value() -> None:
    config = SHConfig()

    new_value = "new"

    config.instance_id = new_value
    assert config.instance_id == new_value, "New value was not set"

    config.reset("sh_base_url")
    config.reset(["aws_access_key_id", "aws_secret_access_key"])
    assert config.instance_id == new_value, "Instance ID should not reset yet"

    config.reset()
    assert config.instance_id == "", "Instance ID should reset"


@pytest.mark.dependency(depends=["test_fake_config_during_tests"])
def test_save(restore_config_file: None) -> None:
    config = SHConfig()
    old_value = config.download_timeout_seconds

    config.download_timeout_seconds = "abcd"  # type: ignore[assignment]
    with pytest.raises(ValueError):
        config.save()

    new_value = 150.5
    config.download_timeout_seconds = new_value

    new_config = SHConfig()
    assert new_config.download_timeout_seconds == old_value, "Value should not have changed"

    config.save()
    config = SHConfig()
    assert config.download_timeout_seconds == new_value, "Saved value should have changed"


@pytest.mark.dependency(depends=["test_fake_config_during_tests"])
@pytest.mark.parametrize("hide_credentials", [True, False])
def test_copy(hide_credentials: bool) -> None:
    config = SHConfig(hide_credentials=hide_credentials)
    config.instance_id = "a"

    copied_config = config.copy()
    assert copied_config is not config
    assert copied_config._hide_credentials == hide_credentials
    assert copied_config.instance_id == config.instance_id

    copied_config.instance_id = "b"
    assert config.instance_id == "a" and copied_config.instance_id == "b"


@pytest.mark.dependency(depends=["test_fake_config_during_tests"])
def test_config_equality(test_config: SHConfig) -> None:
    assert test_config != 42
    assert test_config != test_config.get_config_dict()

    config1 = SHConfig(hide_credentials=False)
    config2 = SHConfig(hide_credentials=True)

    assert config1 is not config2
    assert config1 == config2

    config2.sh_client_id = "something_else"
    assert config1 != config2


@pytest.mark.dependency(depends=["test_fake_config_during_tests"])
def test_raise_for_missing_instance_id(test_config: SHConfig) -> None:
    test_config.raise_for_missing_instance_id()

    test_config.instance_id = ""
    with pytest.raises(ValueError):
        test_config.raise_for_missing_instance_id()


@pytest.mark.dependency(depends=["test_fake_config_during_tests"])
@pytest.mark.parametrize("hide_credentials", [False, True])
def test_config_repr(hide_credentials: bool) -> None:
    config = SHConfig(hide_credentials=hide_credentials)
    config.instance_id = "a" * 20
    config_repr = repr(config)

    assert config_repr.startswith(SHConfig.__name__)

    if hide_credentials:
        assert config.instance_id not in config_repr
        assert "*" * 16 + "a" * 4 in config_repr
    else:
        for param in config.get_params():
            assert f"{param}={repr(getattr(config, param))}" in config_repr


@pytest.mark.dependency(depends=["test_fake_config_during_tests"])
@pytest.mark.parametrize("hide_credentials", [False, True])
def test_get_config_dict(hide_credentials: bool) -> None:
    config = SHConfig(hide_credentials=hide_credentials)
    config.sh_client_secret = "x" * 15
    config.aws_secret_access_key = "y" * 10

    config_dict = config.get_config_dict()
    assert isinstance(config_dict, dict)
    assert tuple(config_dict) == config.get_params()

    if hide_credentials:
        assert config_dict["sh_client_secret"] == "*" * 11 + "x" * 4
        assert config_dict["aws_secret_access_key"] == "*" * 10
    else:
        assert config_dict["sh_client_secret"] == config.sh_client_secret
        assert config_dict["aws_secret_access_key"] == config.aws_secret_access_key


@pytest.mark.dependency(depends=["test_fake_config_during_tests"])
def test_transfer_with_ray(test_config: SHConfig, ray: Any) -> None:
    """This test makes sure that the process of transferring SHConfig object to a Ray worker, working with it, and
    sending it back works correctly.
    """

    def _remote_ray_testing(remote_config: SHConfig) -> SHConfig:
        """Makes a few checks and modifications to the config object"""
        assert repr(remote_config).startswith("SHConfig")
        assert isinstance(remote_config.get_config_dict(), dict)
        assert os.path.exists(remote_config.get_config_location())
        assert remote_config.instance_id == "fake_instance_id"

        remote_config.instance_id = "new_fake_instance_id"
        return remote_config

    config_future = ray.remote(_remote_ray_testing).remote(test_config)
    transferred_config = ray.get(config_future)

    assert repr(test_config).startswith("SHConfig")
    assert transferred_config.instance_id == "new_fake_instance_id"

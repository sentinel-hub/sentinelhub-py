"""
Unit tests for config.py module
"""
import json
import os

import pytest

from sentinelhub import SHConfig


@pytest.fixture(name="restore_config")
def restore_config_fixture():
    """A fixture that makes sure original config is restored after a test is executed. It restores the config even if
    a test has failed.
    """
    original_config = SHConfig()
    yield
    original_config.save()


def test_config_file():
    config = SHConfig()

    config_file = config.get_config_location()
    assert os.path.isfile(config_file), f"Config file does not exist: {os.path.abspath(config_file)}"
    
    
    

    with open(config_file, "r") as fp:
        config_dict = json.load(fp)

    for param, value in config_dict.items():
        if param in config.CREDENTIALS:
            continue

        if isinstance(value, str):
            value = value.rstrip("/")

        assert config[param] == value


def test_reset():
    config = SHConfig()
    default_config = SHConfig(use_defaults=True)

    old_value = config.instance_id
    new_value = "new"
    config.instance_id = new_value
    assert config.instance_id == new_value, "New value was not set"
    assert config["instance_id"] == new_value, "New value was not set"
    assert config._cache["instance_id"] == old_value, "Private value has changed"

    config.reset("sh_base_url")
    config.reset(["aws_access_key_id", "aws_secret_access_key"])
    assert config.instance_id == new_value, "Instance ID should not reset yet"

    config.reset()
    assert config.instance_id == default_config.instance_id, "Instance ID should reset"


def test_save(restore_config):
    config = SHConfig()
    old_value = config.download_timeout_seconds

    config.download_timeout_seconds = "abcd"
    with pytest.raises(ValueError):
        config.save()

    new_value = 150.5
    config.download_timeout_seconds = new_value

    new_config = SHConfig()
    assert new_config.download_timeout_seconds == old_value, "Value should not have changed"

    config.save()
    config = SHConfig()
    assert config.download_timeout_seconds == new_value, "Saved value should have changed"


def test_copy():
    config = SHConfig(hide_credentials=True)
    config.instance_id = "a"

    copied_config = config.copy()
    assert copied_config._hide_credentials
    assert copied_config._cache is config._cache
    assert copied_config.instance_id == config.instance_id

    copied_config.instance_id = "b"
    assert config.instance_id == "a"


def test_config_equality():
    assert SHConfig() != 42

    config1 = SHConfig(hide_credentials=False, use_defaults=True)
    config2 = SHConfig(hide_credentials=True, use_defaults=True)
    
    
    

    assert config1 is not config2
    assert config1 == config2

    config2.sh_client_id = "XXX"
    assert config1 != config2


def test_raise_for_missing_instance_id():
    config = SHConfig()
    

    config.instance_id = "xxx"
    config.raise_for_missing_instance_id()

    config.instance_id = ""
    with pytest.raises(ValueError):
        config.raise_for_missing_instance_id()


@pytest.mark.parametrize("hide_credentials", [False, True])
def test_config_repr(hide_credentials):
    config = SHConfig(hide_credentials=hide_credentials)
    config.instance_id = "a" * 20
    config_repr = repr(config)

    assert config_repr.startswith(SHConfig.__name__)

    if hide_credentials:
        assert config.instance_id not in config_repr
        assert "*" * 16 + "a" * 4 in config_repr
    else:
        for param in config.get_params():
            assert f"{param}={repr(config[param])}" in config_repr


@pytest.mark.parametrize("hide_credentials", [False, True])
def test_get_config_dict(hide_credentials):
    config = SHConfig(hide_credentials=hide_credentials)
    config.sh_client_secret = "x" * 15
    config.aws_secret_access_key = "y" * 10

    config_dict = config.get_config_dict()
    assert isinstance(config_dict, dict)
    assert list(config_dict) == config.get_params()

    if hide_credentials:
        assert config_dict["sh_client_secret"] == "*" * 11 + "x" * 4
        assert config_dict["aws_secret_access_key"] == "*" * 10
    else:
        assert config_dict["sh_client_secret"] == config.sh_client_secret
        assert config_dict["aws_secret_access_key"] == config.aws_secret_access_key


def test_transfer_with_ray(ray):
    """This test makes sure that the process of transferring SHConfig object to a Ray worker, working with it, and
    sending it back works correctly.
    """
    config = SHConfig()
    config.instance_id = "x"

    def _remote_ray_testing(remote_config):
        """Makes a few checks and modifications to the config object"""
        
        
        
        assert repr(remote_config).startswith("SHConfig")
        assert isinstance(remote_config.get_config_dict(), dict)
        
        assert os.path.exists(remote_config.get_config_location())
        assert remote_config.instance_id == "x"

        
        remote_config.instance_id = "y"
        return remote_config
    
    

    config_future = ray.remote(_remote_ray_testing).remote(config)
    
    transferred_config = ray.get(config_future)

    assert repr(config).startswith("SHConfig")
    assert transferred_config.instance_id == "y"

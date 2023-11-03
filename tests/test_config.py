"""
Unit tests for config.py module
"""

import os
import shutil
from typing import Generator

import pytest

from sentinelhub import SHConfig
from sentinelhub.config import DEFAULT_PROFILE, SH_CLIENT_ID_ENV_VAR, SH_CLIENT_SECRET_ENV_VAR, SH_PROFILE_ENV_VAR


@pytest.fixture(autouse=True, scope="module")
def _switch_and_restore_config() -> Generator[None, None, None]:
    """A fixture that makes sure original config is restored after tests are executed. It restores the config even if
    a test has failed.
    """
    config_path = SHConfig.get_config_location()
    cache_path = config_path.replace(".toml", "_test_cache.toml")
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


@pytest.fixture(name="_restore_config_file")
def _restore_config_file_fixture() -> Generator[None, None, None]:
    """A fixture that ensures the config file is reset after the test."""
    with open(SHConfig.get_config_location()) as file:
        content = file.read()
    yield
    with open(SHConfig.get_config_location(), "w") as file:
        file.write(content)


@pytest.fixture(name="dummy_config")
def dummy_config_fixture() -> SHConfig:
    config = SHConfig(use_defaults=True)
    config.instance_id = "fake_instance_id"
    config.sh_client_id = "tester"
    config.sh_client_secret = "1_l1k3-p1n34ppl3*0n%p1224"
    return config


@pytest.mark.dependency()
def test_user_config_is_masked() -> None:
    """Only lets the tests run if the user config has been cached and switched with a dummy one."""
    config = SHConfig()
    credentials_removed = all(getattr(config, field) == "" for field in config.CREDENTIALS)
    assert credentials_removed, "Credentials not properly removed for testing. Aborting tests."


@pytest.mark.dependency(depends=["test_user_config_is_masked"])
def test_config_file_exists() -> None:
    config_file = SHConfig.get_config_location()
    assert os.path.isfile(config_file), f"Config file does not exist: {os.path.abspath(config_file)}"


@pytest.mark.dependency(depends=["test_user_config_is_masked"])
@pytest.mark.usefixtures("_restore_config_file")
def test_save() -> None:
    config = SHConfig()
    old_value = config.download_timeout_seconds

    new_value = 150.5
    config.download_timeout_seconds = new_value

    new_config = SHConfig()
    assert new_config.download_timeout_seconds == old_value, "Value should not have changed"

    config.save()
    config = SHConfig()
    assert config.download_timeout_seconds == new_value, "Saved value should have changed"


@pytest.mark.dependency(depends=["test_user_config_is_masked"])
@pytest.mark.usefixtures("_restore_config_file")
def test_environment_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """We use `monkeypatch` to avoid modifying global environment."""
    config = SHConfig()
    config.sh_client_id = "beepbeep"
    config.sh_client_secret = "imasheep"
    config.save()

    monkeypatch.setenv(SH_CLIENT_ID_ENV_VAR, "beekeeper")
    monkeypatch.setenv(SH_CLIENT_SECRET_ENV_VAR, "bees-are-very-friendly")

    config = SHConfig()
    assert config.sh_client_id == "beekeeper"
    assert config.sh_client_secret == "bees-are-very-friendly"


@pytest.mark.dependency(depends=["test_user_config_is_masked"])
def test_initialization_with_params(monkeypatch: pytest.MonkeyPatch) -> None:
    loaded_config = SHConfig()
    monkeypatch.setenv(SH_CLIENT_ID_ENV_VAR, "beekeeper")

    config = SHConfig(sh_client_id="me", instance_id="beep", geopedia_wms_url="123")
    assert config.instance_id == "beep"
    assert config.geopedia_wms_url != loaded_config.geopedia_wms_url, "Should override settings from config.toml"
    assert config.sh_client_id == "me", "Should override environment variable."


@pytest.mark.dependency(depends=["test_user_config_is_masked"])
@pytest.mark.usefixtures("_restore_config_file")
def test_profiles() -> None:
    config = SHConfig()
    config.instance_id = "beepbeep"
    config.sh_client_id = "beepbeep"  # also some tests with a credentials field
    config.save(profile="beep")

    config.instance_id = "boopboop"
    config.save(profile="boop")

    beep_config = SHConfig(profile="beep")
    assert beep_config.instance_id == "beepbeep"
    assert SHConfig.load(profile="boop").instance_id == "boopboop"

    # save an existing profile
    beep_config.sh_client_id = "bap"
    assert SHConfig(profile="beep").sh_client_id == "beepbeep"
    beep_config.save(profile="beep")
    assert SHConfig(profile="beep").sh_client_id == "bap"


@pytest.mark.dependency(depends=["test_user_config_is_masked"])
@pytest.mark.usefixtures("_restore_config_file")
def test_initialize_nondefault_profile() -> None:
    """Since there is no config, loading a non-default profile should fail."""
    config = SHConfig()
    os.remove(config.get_config_location())

    SHConfig()  # works for default

    os.remove(config.get_config_location())
    with pytest.raises(KeyError):
        SHConfig("mr_president")


@pytest.mark.dependency(depends=["test_user_config_is_masked"])
@pytest.mark.usefixtures("_restore_config_file")
def test_profiles_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """We use `monkeypatch` to avoid modifying global environment."""
    config = SHConfig()
    config.instance_id = "bee"
    config.save(profile="beekeeper")

    assert SHConfig("beekeeper").instance_id == "bee"
    assert SHConfig().instance_id == ""

    monkeypatch.setenv(SH_PROFILE_ENV_VAR, "beekeeper")
    assert SHConfig().instance_id == "bee", "Environment profile is not used."
    assert SHConfig.load().instance_id == "bee", "The load method does not respect the environment profile."
    assert SHConfig(profile=DEFAULT_PROFILE).instance_id == "", "Explicit profile overrides environment."

    config = SHConfig()
    config.instance_id = "many bee"
    config.save()

    assert SHConfig(profile="beekeeper").instance_id == "many bee", "Save method does not respect the env profile."
    assert SHConfig(profile=DEFAULT_PROFILE).instance_id == "", "Saving with env profile changed default profile."


def test_loading_unknown_profile_fails() -> None:
    with pytest.raises(KeyError):
        SHConfig.load(profile="does not exist")


@pytest.mark.dependency(depends=["test_user_config_is_masked"])
def test_copy(dummy_config: SHConfig) -> None:
    dummy_config.instance_id = "a"

    copied_config = dummy_config.copy()
    assert copied_config is not dummy_config
    assert copied_config == dummy_config

    copied_config.instance_id = "b"
    assert dummy_config.instance_id == "a"
    assert copied_config.instance_id == "b"


@pytest.mark.dependency(depends=["test_user_config_is_masked"])
def test_config_equality() -> None:
    config1, config2 = SHConfig(), SHConfig()

    assert config1 is not config2
    assert config1 == config2
    assert config1 != config1.to_dict(mask_credentials=False)

    config2.sh_client_id = "something_else"
    assert config1 != config2


@pytest.mark.dependency(depends=["test_user_config_is_masked"])
def test_config_repr() -> None:
    config = SHConfig()
    config.instance_id = "a" * 20
    config_repr = repr(config)

    assert config_repr.startswith(SHConfig.__name__)

    assert config.instance_id not in config_repr, "Credentials are not masked properly."
    assert "*" * 16 + "a" * 4 in config_repr, "Credentials are not masked properly."

    for param in config.to_dict():
        if param not in SHConfig.CREDENTIALS:
            assert f"{param}={getattr(config, param)!r}" in config_repr


@pytest.mark.dependency(depends=["test_user_config_is_masked"])
@pytest.mark.parametrize("hide_credentials", [False, True])
def test_transformation_to_dict(hide_credentials: bool) -> None:
    config = SHConfig()
    config.sh_client_secret = "x" * 15
    config.aws_secret_access_key = "y" * 10

    config_dict = config.to_dict(hide_credentials)
    assert isinstance(config_dict, dict)

    if hide_credentials:
        assert config_dict["sh_client_secret"] == "*" * 11 + "x" * 4
        assert config_dict["aws_secret_access_key"] == "*" * 10
    else:
        assert config_dict["sh_client_secret"] == config.sh_client_secret
        assert config_dict["aws_secret_access_key"] == config.aws_secret_access_key

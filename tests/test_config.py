"""
Unit tests for config.py module
"""
import json
import os

import pytest

from sentinelhub import SHConfig


@pytest.fixture(name='restore_config')
def restore_config_fixture():
    """ A fixture that makes sure original config is restored after a test is executed. It restores the config even if
    a test has failed.
    """
    original_config = SHConfig()
    yield
    original_config.save()


def test_config_file():
    config = SHConfig()

    config_file = config.get_config_location()
    assert os.path.isfile(config_file), f'Config file does not exist: {os.path.abspath(config_file)}'

    with open(config_file, 'r') as fp:
        config_dict = json.load(fp)

    for param, value in config_dict.items():
        if param in config._instance.CREDENTIALS:
            continue

        if isinstance(value, str):
            value = value.rstrip('/')

        assert config[param] == value


def test_reset():
    config = SHConfig()

    old_value = config.instance_id
    new_value = 'new'
    config.instance_id = new_value
    assert config.instance_id == new_value, 'New value was not set'
    assert config['instance_id'] == new_value, 'New value was not set'
    assert config._instance.instance_id == old_value, 'Private value has changed'

    config.reset('sh_base_url')
    config.reset(['aws_access_key_id', 'aws_secret_access_key'])
    assert config.instance_id == new_value, 'Instance ID should not reset yet'

    config.reset()
    assert config.instance_id == config._instance.CONFIG_PARAMS['instance_id'], 'Instance ID should reset'


def test_save(restore_config):
    config = SHConfig()

    config.download_timeout_seconds = 'abcd'
    with pytest.raises(ValueError):
        config.save()

    new_value = 150.5
    config.download_timeout_seconds = new_value
    config.save()
    config = SHConfig()
    assert config.download_timeout_seconds == new_value, 'Saved value should have changed'


def test_raise_for_missing_instance_id():
    config = SHConfig()

    config.instance_id = 'xxx'
    config.raise_for_missing_instance_id()

    config.instance_id = ''
    with pytest.raises(ValueError):
        config.raise_for_missing_instance_id()


@pytest.mark.parametrize('hide_credentials', [False, True])
def test_config_repr(hide_credentials):
    config = SHConfig(hide_credentials=hide_credentials)
    config.instance_id = 'a' * 20
    config_repr = repr(config)

    assert config_repr.startswith(SHConfig.__name__)

    if hide_credentials:
        assert config.instance_id not in config_repr
        assert '*' * 16 + 'a' * 4 in config_repr
    else:
        for param in config.get_params():
            assert f'{param}={repr(config[param])}' in config_repr


@pytest.mark.parametrize('hide_credentials', [False, True])
def test_get_config_dict(hide_credentials):
    config = SHConfig(hide_credentials=hide_credentials)
    config.sh_client_secret = 'x' * 15
    config.aws_secret_access_key = 'y' * 10

    config_dict = config.get_config_dict()
    assert isinstance(config_dict, dict)
    assert list(config_dict) == config.get_params()

    if hide_credentials:
        assert config_dict['sh_client_secret'] == '*' * 11 + 'x' * 4
        assert config_dict['aws_secret_access_key'] == '*' * 10
    else:
        assert config_dict['sh_client_secret'] == config.sh_client_secret
        assert config_dict['aws_secret_access_key'] == config.aws_secret_access_key

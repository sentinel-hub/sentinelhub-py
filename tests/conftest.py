"""
Module with global fixtures
"""
import os

import pytest

from sentinelhub import SHConfig


@pytest.fixture(name='config')
def config_fixture():
    config = SHConfig()
    for param in config.get_params():
        env_variable = param.upper()
        if os.environ.get(env_variable):
            setattr(config, param, os.environ.get(env_variable))
    return config

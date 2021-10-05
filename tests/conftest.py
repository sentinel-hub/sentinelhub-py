"""
Module with global fixtures
"""
import os
import shutil
import logging

import pytest

from sentinelhub import SHConfig

pytest.register_assert_rewrite('sentinelhub.testing_utils')
from sentinelhub.testing_utils import get_input_folder, get_output_folder

INPUT_FOLDER = get_input_folder(__file__)
OUTPUT_FOLDER = get_output_folder(__file__)


def pytest_configure(config):
    shconfig = SHConfig()
    for param in shconfig.get_params():
        env_variable = param.upper()
        if os.environ.get(env_variable):
            setattr(shconfig, param, os.environ.get(env_variable))
    shconfig.save()


@pytest.fixture(name='config')
def config_fixture():
    return SHConfig()


@pytest.fixture(name='input_folder')
def input_folder_fixture():
    return INPUT_FOLDER


@pytest.fixture(name='output_folder')
def output_folder_fixture():
    """ Creates the necessary folder and cleans up after test is done. """
    if not os.path.exists(OUTPUT_FOLDER):
        os.mkdir(OUTPUT_FOLDER)
    yield OUTPUT_FOLDER
    shutil.rmtree(OUTPUT_FOLDER, ignore_errors=True)


@pytest.fixture(name='logger')
def logger_fixture():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s %(module)s:%(lineno)d [%(levelname)s] %(funcName)s  %(message)s'
    )
    return logging.getLogger(__name__)

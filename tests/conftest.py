"""
Module with global fixtures
"""

import logging
import os
import shutil
from typing import Any, Generator

import pytest

from sentinelhub import SentinelHubSession, SHConfig

pytest.register_assert_rewrite("sentinelhub.testing_utils")
from sentinelhub.testing_utils import get_input_folder, get_output_folder  # noqa: E402

INPUT_FOLDER = get_input_folder(__file__)
OUTPUT_FOLDER = get_output_folder(__file__)


def pytest_configure() -> None:
    shconfig = SHConfig()
    for param in shconfig.to_dict():
        env_variable = param.upper()
        if os.environ.get(env_variable):
            setattr(shconfig, param, os.environ.get(env_variable))
    shconfig.save()


@pytest.fixture(name="config")
def config_fixture() -> SHConfig:
    return SHConfig()


@pytest.fixture(name="input_folder", scope="session")
def input_folder_fixture() -> str:
    return INPUT_FOLDER


@pytest.fixture(name="output_folder")
def output_folder_fixture() -> Generator[str, None, None]:
    """Creates the necessary folder and cleans up after test is done."""
    if not os.path.exists(OUTPUT_FOLDER):
        os.mkdir(OUTPUT_FOLDER)
    yield OUTPUT_FOLDER
    shutil.rmtree(OUTPUT_FOLDER, ignore_errors=True)


@pytest.fixture(name="logger")
def logger_fixture() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)-15s %(module)s:%(lineno)d [%(levelname)s] %(funcName)s  %(message)s"
    )
    return logging.getLogger(__name__)


@pytest.fixture(name="session", scope="session")
def session_fixture() -> SentinelHubSession:
    return SentinelHubSession()


@pytest.fixture(name="ray")
def ray_fixture() -> Generator[Any, None, None]:
    """Ensures that the ray server will stop even if test fails"""
    ray = pytest.importorskip("ray")
    ray.init(log_to_driver=False)

    yield ray
    ray.shutdown()

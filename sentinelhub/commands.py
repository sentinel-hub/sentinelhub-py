"""
Module that implements command line interface for the package
"""

from __future__ import annotations

import json
from typing import Any, Callable, TypeVar

import click

from .config import SHConfig
from .download import DownloadClient, DownloadRequest

FC = TypeVar("FC", bound=Callable[..., Any])


@click.command()
def main_help() -> None:
    """
    Welcome to sentinelhub Python library command line help.

    \b
    There are multiple modules with command line functionality:\n
       - sentinelhub.aws \n
       - sentinelhub.config \n
       - sentinelhub.download \n

    To check more about certain module command use: \n
      sentinelhub.<module name> --help
    """


def _config_options(func: FC) -> FC:
    """A helper function which joins `click.option` functions of each parameter from `SHConfig`."""
    for param in list(SHConfig().to_dict())[-1::-1]:
        func = click.option(f"--{param}", param, help=f"Set new values to configuration parameter `{param}`")(func)
    return func


@click.command()
@click.option("--show", is_flag=True, default=False, help="Show current configuration")
@click.option("--profile", default=None, help="Selects profile to show/configure.")
@_config_options
def config(show: bool, profile: str | None, **params: Any) -> None:
    """Inspect and configure parameters in your local sentinelhub configuration file

    \b
    Example:
      sentinelhub.config --show
      sentinelhub.config --instance_id <new instance id>
      sentinelhub.config --max_download_attempts 5 --download_sleep_time 20 --download_timeout_seconds 120
    """
    sh_config = SHConfig(profile=profile)
    old_config = sh_config.copy()

    for param, value in params.items():
        if value is not None:
            try:
                value = int(value)
            except ValueError:
                if value.lower() == "true":
                    value = True
                elif value.lower() == "false":
                    value = False
            if getattr(sh_config, param) != value:
                setattr(sh_config, param, value)

    sh_config.save(profile=profile)

    for param, value in sh_config.to_dict(mask_credentials=False).items():
        if value != getattr(old_config, param):
            click.echo(f"The value of parameter `{param}` was updated to {value!r}")

    if show:
        unmasked_str_repr = json.dumps(sh_config.to_dict(mask_credentials=False), indent=2)
        click.echo(unmasked_str_repr)
        click.echo(f"Configuration file location: {sh_config.get_config_location()}")


@click.command()
@click.argument("url")
@click.argument("filename", type=click.Path())
@click.option("-r", "--redownload", is_flag=True, default=False, help="Redownload existing files")
def download(url: str, filename: str, redownload: bool) -> None:
    """Download from custom created URL into custom created file path

    \b
    Example:
    sentinelhub.download https://roda.sentinel-hub.com/sentinel-s2-l1c/tiles/36/M/ZB/2022/3/17/0/metadata.xml \
./data/example.xml
    """
    data_folder, filename = filename.rsplit("/", 1)
    download_list = [
        DownloadRequest(url=url, data_folder=data_folder, filename=filename, save_response=True, return_data=False)
    ]
    DownloadClient(redownload=redownload).download(download_list)

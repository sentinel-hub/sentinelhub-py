"""
Module that implements command line interface for the package
"""

import click

from .config import SHConfig
from .constants import DataSource
from .data_request import get_safe_format, download_safe_format
from .download import DownloadRequest, DownloadClient


@click.command()
def main_help():
    """
    Welcome to sentinelhub Python library command line help.

    \b
    There are multiple modules with command line functionality:\n
       - sentinelhub.aws \n
       - senitnelhub.config \n
       - sentinelhub.download \n

    To check more about certain module command use: \n
      sentinelhub.<module name> --help
    """


@click.command()
@click.option('--product', help='Product ID input')
@click.option('--tile', nargs=2, help='Tile name and date input')
@click.option('-f', '--folder', default='.', help='Set download location')
@click.option('-r', '--redownload', is_flag=True, default=False, help='Redownload existing files')
@click.option('-i', '--info', is_flag=True, default=False, help='Return safe format structure')
@click.option('-e', '--entire', is_flag=True, default=False, help='Get entire product of specified tile')
@click.option('-b', '--bands', default=None, help='Comma separated list (no spaces) of bands to retrieve')
@click.option('--l2a', is_flag=True, default=False, help='In case of tile request this flag specifies L2A products')
def aws(product, tile, folder, redownload, info, entire, bands, l2a):
    """Download Sentinel-2 data from Sentinel-2 on AWS to ESA SAFE format. Download uses multiple threads.

    \b
    Examples with Sentinel-2 L1C data:
      sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551
      sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 -i
      sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 -f /home/ESA_Products
      sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 --bands B08,B11
      sentinelhub.aws --tile T54HVH 2017-04-14
      sentinelhub.aws --tile T54HVH 2017-04-14 -e

    \b
    Examples with Sentinel-2 L2A data:
      sentinelhub.aws --product S2A_MSIL2A_20180402T151801_N0207_R068_T33XWJ_20180402T202222
      sentinelhub.aws --tile T33XWJ 2018-04-02 --l2a
    """
    band_list = None if bands is None else bands.split(',')
    data_source = DataSource.SENTINEL2_L2A if l2a else DataSource.SENTINEL2_L1C
    if info:
        if product is None:
            click.echo(get_safe_format(tile=tile, entire_product=entire, data_source=data_source))
        else:
            click.echo(get_safe_format(product_id=product))
    else:
        if product is None:
            download_safe_format(tile=tile, folder=folder, redownload=redownload, entire_product=entire,
                                 bands=band_list, data_source=data_source)
        else:
            download_safe_format(product_id=product, folder=folder, redownload=redownload, bands=band_list)


def _config_options(func):
    """ A helper function which joins click.option functions of each parameter from config.json
    """
    for param in SHConfig().get_params()[-1::-1]:
        func = click.option('--{}'.format(param), param,
                            help='Set new values to configuration parameter "{}"'.format(param))(func)
    return func


@click.command()
@click.option('--show', is_flag=True, default=False, help='Show current configuration')
@click.option('--reset', is_flag=True, default=False, help='Reset configuration to initial state')
@_config_options
def config(show, reset, **params):
    """Inspect and configure parameters in your local sentinelhub configuration file

    \b
    Example:
      sentinelhub.config --show
      sentinelhub.config --instance_id <new instance id>
      sentinelhub.config --max_download_attempts 5 --download_sleep_time 20 --download_timeout_seconds 120
    """
    sh_config = SHConfig()

    if reset:
        sh_config.reset()

    for param, value in params.items():
        if value is not None:
            try:
                value = int(value)
            except ValueError:
                if value.lower() == 'true':
                    value = True
                elif value.lower() == 'false':
                    value = False
            if getattr(sh_config, param) != value:
                setattr(sh_config, param, value)

    old_config = SHConfig()
    sh_config.save()

    for param in sh_config.get_params():
        if sh_config[param] != old_config[param]:
            value = sh_config[param]
            if isinstance(value, str):
                value = "'{}'".format(value)
            click.echo("The value of parameter '{}' was updated to {}".format(param, value))

    if show:
        click.echo(str(sh_config))
        click.echo('Configuration file location: {}'.format(sh_config.get_config_location()))


@click.command()
@click.argument('url')
@click.argument('filename', type=click.Path())
@click.option('-r', '--redownload', is_flag=True, default=False, help='Redownload existing files')
def download(url, filename, redownload):
    """Download from custom created URL into custom created file path

    \b
    Example:
    sentinelhub.download http://sentinel-s2-l1c.s3.amazonaws.com/tiles/54/H/VH/2017/4/14/0/metadata.xml home/example.xml
    """
    data_folder, filename = filename.rsplit('/', 1)
    download_list = [DownloadRequest(url=url, data_folder=data_folder, filename=filename, save_response=True,
                                     return_data=False)]
    DownloadClient(redownload=redownload).download(download_list)

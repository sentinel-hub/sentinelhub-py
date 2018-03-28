import click

from .data_request import get_safe_format, download_safe_format
from .download import download_data, DownloadRequest
from .config import SHConfig

# pylint: disable=unused-argument


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
    pass


@click.command()
@click.option('--product', help='Product ID input')
@click.option('--tile', nargs=2, help='Tile name and date input')
@click.option('-f', '--folder', default='.', help='Set download location')
@click.option('-r', '--redownload', is_flag=True, default=False, help='Redownload existing files')
@click.option('-i', '--info', is_flag=True, default=False, help='Return safe format structure')
@click.option('-e', '--entire', is_flag=True, default=False, help='Get entire product of specified tile')
@click.option('-b', '--bands', default=None, help='Comma separated list (no spaces) of bands to retrieve')
def aws(product, tile, folder, redownload, info, entire, bands):
    """Download Sentinel-2 data from Sentinel-2 on AWS to ESA SAFE format. Download uses multiple threads.

    \b
    Examples:
      sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 \n
      sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 -i \n
      sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 -f /home/ESA_Products \n
      sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 --bands B08,B11 \n
      sentinelhub.aws --tile T54HVH 2017-04-14 \n
      sentinelhub.aws --tile T54HVH 2017-04-14 -e \n
    """
    band_list = None if bands is None else bands.split(',')
    if info:
        if product is None:
            click.echo(get_safe_format(tile=tile, entire_product=entire))
        else:
            click.echo(get_safe_format(product_id=product))
    else:
        if product is None:
            download_safe_format(tile=tile, folder=folder, redownload=redownload,
                                 entire_product=entire, bands=band_list)
        else:
            download_safe_format(product_id=product, folder=folder, redownload=redownload, bands=band_list)


def config_options(f):
    for param in SHConfig().get_params():
        f = click.option('--{}'.format(param), param,
                         help='Set new values to configuration parameter "{}"'.format(param))(f)
    return f


@click.command()
@click.option('--show', is_flag=True, default=False, help='Show current configurations')
@config_options
def config(show, **params):
    """Inspect and configure parameters in your local sentinelhub configuration file

    \b
    Example:
      sentinelhub.config --show
      sentinelhub.config --instance_id <new instance id>
      sentinelhub.config --max_download_attempts 5 --download_sleep_time 20 --download_timeout_seconds 120
    """
    sh_config = SHConfig()
    for param, value in params.items():
        if value is not None:
            try:
                value = int(value)
            except ValueError:
                pass
            setattr(sh_config, param, value)
    sh_config.save()

    if show:
        click.echo(str(sh_config))


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
    download_list = [DownloadRequest(url=url, data_folder='', filename=filename, save_response=True, return_data=False)]
    download_data(download_list, redownload=redownload)

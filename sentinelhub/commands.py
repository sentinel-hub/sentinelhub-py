import click

from sentinelhub import download_safe_format, get_safe_format

@click.command()
@click.argument('productID')
@click.option('-f', '--folder', default='.', type=click.Path(exists=True), help='Set download location')
@click.option('-r', '--redownload', is_flag=True, default=False, help='Redownload existing files')
@click.option('-t', '--threaded', is_flag=True, default=False, help='Use threaded download')
@click.option('-i', '--info', is_flag=True, default=False, help='Return safe format structure')
#@click.option('-')
def main(productid, folder, redownload, threaded, info):
    """Download Sentinel-2 data from Sentinel-2 on AWS to ESA SAFE format.

    \b
    Examples:
      sentinelhub S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551
      sentinelhub S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 -irt
      sentinelhub S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 -f /home/ESA_Products
    """

    if info:
        click.echo(get_safe_format(productid))
    download_safe_format(productid, folder=folder, redownload=redownload, threadedDownload=threaded)

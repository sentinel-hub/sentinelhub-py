import click

from sentinelhub import download_safe_format

@click.command()
@click.argument('productID')
def download(productid):
    """Download Sentinel-2 data from Sentinel-2 on AWS to ESA SAFE format.
    
    \b
    Example:
      sentinelhub S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551
    """
    download_safe_format(productid)

"""
Module that implements command line interface for AWS package functionalities
"""
import click

from ..data_collections import DataCollection
from .request import download_safe_format, get_safe_format


@click.command()
@click.option("--product", help="Product ID input")
@click.option("--tile", nargs=2, help="Tile name and date input")
@click.option("-f", "--folder", default=".", help="Set download location")
@click.option("-r", "--redownload", is_flag=True, default=False, help="Redownload existing files")
@click.option("-i", "--info", is_flag=True, default=False, help="Return safe format structure")
@click.option("-e", "--entire", is_flag=True, default=False, help="Get entire product of specified tile")
@click.option("-b", "--bands", default=None, help="Comma separated list (no spaces) of bands to retrieve")
@click.option("--l2a", is_flag=True, default=False, help="In case of tile request this flag specifies L2A products")
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
    band_list = None if bands is None else bands.split(",")
    data_collection = DataCollection.SENTINEL2_L2A if l2a else DataCollection.SENTINEL2_L1C
    if info:
        if product is None:
            click.echo(get_safe_format(tile=tile, entire_product=entire, data_collection=data_collection))
        else:
            click.echo(get_safe_format(product_id=product))
    else:
        if product is None:
            download_safe_format(
                tile=tile,
                folder=folder,
                redownload=redownload,
                entire_product=entire,
                bands=band_list,
                data_collection=data_collection,
            )
        else:
            download_safe_format(product_id=product, folder=folder, redownload=redownload, bands=band_list)

"""
Data request interface for downloading satellite data from AWS
"""
from abc import abstractmethod

from ..base import DataRequest
from .client import AwsDownloadClient
from .data import AwsProduct, AwsTile
from .data_safe import SafeProduct, SafeTile


class AwsRequest(DataRequest):
    """The base class for Amazon Web Service request classes. Common parameters are defined here.

    Collects and provides data from AWS.

    More information about Sentinel-2 AWS registry: https://registry.opendata.aws/sentinel-2/
    """

    def __init__(self, *, bands=None, metafiles=None, safe_format=False, **kwargs):
        """
        :param bands: List of Sentinel-2 bands for request. If `None` all bands will be obtained
        :type bands: list(str) or None
        :param metafiles: list of additional metafiles available on AWS
                          (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``)
        :type metafiles: list(str)
        :param safe_format: flag that determines the structure of saved data. If `True` it will be saved in .SAFE format
                            defined by ESA. If `False` it will be saved in the same structure as the structure at AWS.
        :type safe_format: bool
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.bands = bands
        self.metafiles = metafiles
        self.safe_format = safe_format

        self.aws_service = None
        super().__init__(AwsDownloadClient, **kwargs)

    @abstractmethod
    def create_request(self):
        raise NotImplementedError

    def get_aws_service(self):
        """
        :return: initialized AWS service class
        :rtype: aws.AwsProduct or aws.AwsTile or aws_safe.SafeProduct or aws_safe.SafeTile
        """
        return self.aws_service


class AwsProductRequest(AwsRequest):
    """AWS Service request class for an ESA product."""

    def __init__(self, product_id, *, tile_list=None, **kwargs):
        """
        :param product_id: original ESA product identification string
            (e.g. ``'S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551'``)
        :type product_id: str
        :param tile_list: list of tiles inside the product to be downloaded. If parameter is set to `None` all
            tiles inside the product will be downloaded.
        :type tile_list: list(str) or None
        :param bands: List of Sentinel-2 bands for request. If `None` all bands will be obtained
        :type bands: list(str) or None
        :param metafiles: list of additional metafiles available on AWS
            (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``)
        :type metafiles: list(str)
        :param safe_format: flag that determines the structure of saved data. If `True` it will be saved in .SAFE format
            defined by ESA. If `False` it will be saved in the same structure as the structure at AWS.
        :type safe_format: bool
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.product_id = product_id
        self.tile_list = tile_list

        super().__init__(**kwargs)

    def create_request(self):
        if self.safe_format:
            self.aws_service = SafeProduct(
                self.product_id,
                tile_list=self.tile_list,
                bands=self.bands,
                metafiles=self.metafiles,
                config=self.config,
            )
        else:
            self.aws_service = AwsProduct(
                self.product_id,
                tile_list=self.tile_list,
                bands=self.bands,
                metafiles=self.metafiles,
                config=self.config,
            )

        self.download_list, self.folder_list = self.aws_service.get_requests()


class AwsTileRequest(AwsRequest):
    """AWS Service request class for an ESA tile."""

    def __init__(self, *, data_collection, tile=None, time=None, aws_index=None, **kwargs):
        """
        :param data_collection: A collection of requested AWS data. Supported collections are Sentinel-2 L1C and
            Sentinel-2 L2A.
        :type data_collection: DataCollection
        :param tile: tile name (e.g. ``'T10UEV'``)
        :type tile: str
        :param time: tile sensing time in ISO8601 format
        :type time: str
        :param aws_index: there exist Sentinel-2 tiles with the same tile and time parameter. Therefore, each tile on
            AWS also has an index which is visible in their url path. If aws_index is set to `None` the class
            will try to find the index automatically. If there will be multiple choices it will choose the
            lowest index and inform the user.
        :type aws_index: int or None
        :param bands: List of Sentinel-2 bands for request. If `None` all bands will be obtained
        :type bands: list(str) or None
        :param metafiles: list of additional metafiles available on AWS
            (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``)
        :type metafiles: list(str)
        :param safe_format: flag that determines the structure of saved data. If `True` it will be saved in .SAFE
            format defined by ESA. If `False` it will be saved in the same structure as the structure at AWS.
        :type safe_format: bool
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.data_collection = data_collection
        self.tile = tile
        self.time = time
        self.aws_index = aws_index

        super().__init__(**kwargs)

    def create_request(self):
        if self.safe_format:
            self.aws_service = SafeTile(
                self.tile,
                self.time,
                self.aws_index,
                bands=self.bands,
                metafiles=self.metafiles,
                data_collection=self.data_collection,
                config=self.config,
            )
        else:
            self.aws_service = AwsTile(
                self.tile,
                self.time,
                self.aws_index,
                bands=self.bands,
                metafiles=self.metafiles,
                data_collection=self.data_collection,
                config=self.config,
            )

        self.download_list, self.folder_list = self.aws_service.get_requests()


def get_safe_format(product_id=None, tile=None, entire_product=False, bands=None, data_collection=None):
    """Returns .SAFE format structure in form of nested dictionaries. Either ``product_id`` or ``tile`` must be
    specified.

    :param product_id: original ESA product identification string. Default is `None`
    :type product_id: str
    :param tile: tuple containing tile name and sensing time/date. Default is `None`
    :type tile: (str, str)
    :param entire_product: in case tile is specified this flag determines if it will be place inside a .SAFE structure
        of the product. Default is `False`
    :type entire_product: bool
    :param bands: list of bands to download. If `None` all bands will be downloaded. Default is `None`
    :type bands: list(str) or None
    :param data_collection: In case of tile request the collection of satellite data has to be specified.
    :type data_collection: DataCollection
    :return: Nested dictionaries representing .SAFE structure.
    :rtype: dict
    """
    entire_product = entire_product and product_id is None
    if tile is not None:
        safe_tile = SafeTile(tile_name=tile[0], time=tile[1], bands=bands, data_collection=data_collection)
        if not entire_product:
            return safe_tile.get_safe_struct()
        product_id = safe_tile.get_product_id()
    if product_id is None:
        raise ValueError("Either product_id or tile must be specified")
    safe_product = (
        SafeProduct(product_id, tile_list=[tile[0]], bands=bands)
        if entire_product
        else SafeProduct(product_id, bands=bands)
    )
    return safe_product.get_safe_struct()


def download_safe_format(
    product_id=None, tile=None, folder=".", redownload=False, entire_product=False, bands=None, data_collection=None
):
    """Downloads .SAFE format structure in form of nested dictionaries. Either ``product_id`` or ``tile`` must
    be specified.

    :param product_id: original ESA product identification string. Default is `None`
    :type product_id: str
    :param tile: tuple containing tile name and sensing time/date. Default is `None`
    :type tile: (str, str)
    :param folder: location of the directory where the fetched data will be saved. Default is ``'.'``
    :type folder: str
    :param redownload: if `True`, download again the requested data even though it's already saved to disk. If
        `False`, do not download if data is already available on disk. Default is `False`
    :type redownload: bool
    :param entire_product: in case tile is specified this flag determines if it will be place inside a .SAFE structure
        of the product. Default is `False`
    :type entire_product: bool
    :param bands: list of bands to download. If `None` all bands will be downloaded. Default is `None`
    :type bands: list(str) or None
    :param data_collection: In case of tile request the collection of satellite data has to be specified.
    :type data_collection: DataCollection
    :return: Nested dictionaries representing .SAFE structure.
    :rtype: dict
    """
    safe_request = None
    entire_product = entire_product and product_id is None
    if tile is not None:
        safe_request = AwsTileRequest(
            tile=tile[0],
            time=tile[1],
            data_folder=folder,
            bands=bands,
            safe_format=True,
            data_collection=data_collection,
        )
        if entire_product:
            safe_tile = safe_request.get_aws_service()
            product_id = safe_tile.get_product_id()
    if product_id is not None:
        safe_request = (
            AwsProductRequest(product_id, tile_list=[tile[0]], data_folder=folder, bands=bands, safe_format=True)
            if entire_product
            else AwsProductRequest(product_id, data_folder=folder, bands=bands, safe_format=True)
        )

    if safe_request is None:
        raise ValueError("Either 'product_id' or 'tile' has to be defined")

    safe_request.save_data(redownload=redownload)

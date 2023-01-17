"""
Data request interface for downloading satellite data from AWS
"""
import functools
from abc import abstractmethod
from typing import Any, Generic, List, Optional, Tuple, TypeVar, Union

from ..base import DataRequest
from ..data_collections import DataCollection
from ..exceptions import deprecated_class
from .client import AwsDownloadClient
from .data import REQUESTER_PAYS_PARAMS, AwsProduct, AwsTile
from .data_safe import SafeProduct, SafeTile

T = TypeVar("T")


class _BaseAwsDataRequest(DataRequest, Generic[T]):
    """The base class for Amazon Web Service request classes. Common parameters are defined here.

    Collects and provides data from AWS.

    More information about Sentinel-2 AWS registry: https://registry.opendata.aws/sentinel-2/
    """

    def __init__(
        self,
        *,
        bands: Union[None, str, List[str]] = None,
        metafiles: Union[None, str, List[str]] = None,
        safe_format: bool = False,
        **kwargs: Any
    ):
        """
        :param bands: List of Sentinel-2 bands for request. If `None` all bands will be obtained
        :param metafiles: list of additional metafiles available on AWS
                          (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``)
        :param safe_format: flag that determines the structure of saved data. If `True` it will be saved in .SAFE format
                            defined by ESA. If `False` it will be saved in the same structure as the structure at AWS.
        :param data_folder: location of the directory where the fetched data will be saved.
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        self.bands = bands
        self.metafiles = metafiles
        self.safe_format = safe_format

        self.aws_service: T

        client_class = functools.partial(AwsDownloadClient, boto_params=REQUESTER_PAYS_PARAMS)
        super().__init__(client_class, **kwargs)

    @abstractmethod
    def create_request(self) -> None:
        raise NotImplementedError

    def get_aws_service(self) -> T:
        """
        :return: initialized AWS service class
        """
        return self.aws_service


@deprecated_class(message_suffix="It will remain in the codebase for now, but won't be actively maintained.")
class AwsProductRequest(_BaseAwsDataRequest[AwsProduct]):
    """AWS Service request class for an ESA product."""

    def __init__(self, product_id: str, *, tile_list: Optional[List[str]] = None, **kwargs: Any):
        """
        :param product_id: original ESA product identification string
            (e.g. ``'S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551'``)
        :param tile_list: list of tiles inside the product to be downloaded. If parameter is set to `None` all
            tiles inside the product will be downloaded.
        :param bands: List of Sentinel-2 bands for request. If `None` all bands will be obtained
        :param metafiles: list of additional metafiles available on AWS
            (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``)
        :param safe_format: flag that determines the structure of saved data. If `True` it will be saved in .SAFE format
            defined by ESA. If `False` it will be saved in the same structure as the structure at AWS.
        :param data_folder: location of the directory where the fetched data will be saved.
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        self.product_id = product_id
        self.tile_list = tile_list

        super().__init__(**kwargs)

    def create_request(self) -> None:
        product_class = SafeProduct if self.safe_format else AwsProduct
        self.aws_service = product_class(
            self.product_id,
            tile_list=self.tile_list,
            bands=self.bands,
            metafiles=self.metafiles,
            config=self.config,
        )

        self.download_list, self.folder_list = self.aws_service.get_requests()


@deprecated_class(message_suffix="It will remain in the codebase for now, but won't be actively maintained.")
class AwsTileRequest(_BaseAwsDataRequest[AwsTile]):
    """AWS Service request class for an ESA tile."""

    def __init__(
        self,
        *,
        data_collection: DataCollection,
        tile: Optional[str] = None,
        time: Optional[str] = None,
        aws_index: Optional[int] = None,
        **kwargs: Any
    ):
        """
        :param data_collection: A collection of requested AWS data. Supported collections are Sentinel-2 L1C and
            Sentinel-2 L2A.
        :param tile: tile name (e.g. ``'T10UEV'``)
        :param time: tile sensing time in ISO8601 format
        :param aws_index: there exist Sentinel-2 tiles with the same tile and time parameter. Therefore, each tile on
            AWS also has an index which is visible in their url path. If aws_index is set to `None` the class
            will try to find the index automatically. If there will be multiple choices it will choose the
            lowest index and inform the user.
        :param bands: List of Sentinel-2 bands for request. If `None` all bands will be obtained
        :param metafiles: list of additional metafiles available on AWS
            (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``)
        :param safe_format: flag that determines the structure of saved data. If `True` it will be saved in .SAFE
            format defined by ESA. If `False` it will be saved in the same structure as the structure at AWS.
        :param data_folder: location of the directory where the fetched data will be saved.
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        self.data_collection = data_collection
        self.tile = tile
        self.time = time
        self.aws_index = aws_index

        super().__init__(**kwargs)

    def create_request(self) -> None:
        if self.tile is None or self.time is None:
            raise ValueError("The parameters `tile` and `time` must be set.")

        tile_class = SafeTile if self.safe_format else AwsTile
        self.aws_service = tile_class(
            self.tile,
            self.time,
            self.aws_index,
            bands=self.bands,
            metafiles=self.metafiles,
            data_collection=self.data_collection,
            config=self.config,
        )

        self.download_list, self.folder_list = self.aws_service.get_requests()


def get_safe_format(
    product_id: Optional[str] = None,
    tile: Optional[Tuple[str, str]] = None,
    entire_product: bool = False,
    bands: Union[None, str, List[str]] = None,
    data_collection: Optional[DataCollection] = None,
) -> dict:
    """Returns .SAFE format structure in form of nested dictionaries. Either ``product_id`` or ``tile`` must be
    specified.

    :param product_id: original ESA product identification string. Default is `None`
    :param tile: tuple containing tile name and sensing time/date. Default is `None`
    :param entire_product: in case tile is specified this flag determines if it will be place inside a .SAFE structure
        of the product. Default is `False`
    :param bands: list of bands to download. If `None` all bands will be downloaded. Default is `None`
    :param data_collection: In case of tile request the collection of satellite data has to be specified.
    :return: Nested dictionaries representing .SAFE structure.
    """
    entire_product = entire_product and product_id is None
    if tile is not None:
        safe_tile = SafeTile(tile_name=tile[0], time=tile[1], bands=bands, data_collection=data_collection)
        if not entire_product:
            return safe_tile.get_safe_struct()
        product_id = safe_tile.get_product_id()
    if product_id is None:
        raise ValueError("Either product_id or tile must be specified")
    if entire_product:
        if tile is None:
            raise ValueError("The tile parameter must be set.")
        safe_product = SafeProduct(product_id, tile_list=[tile[0]], bands=bands)
    else:
        safe_product = SafeProduct(product_id, bands=bands)

    return safe_product.get_safe_struct()


def download_safe_format(
    product_id: Optional[str] = None,
    tile: Optional[Tuple[str, str]] = None,
    folder: str = ".",
    redownload: bool = False,
    entire_product: bool = False,
    bands: Optional[List[str]] = None,
    data_collection: Optional[DataCollection] = None,
) -> None:
    """Downloads .SAFE format structure in form of nested dictionaries. Either ``product_id`` or ``tile`` must
    be specified.

    :param product_id: original ESA product identification string. Default is `None`
    :param tile: tuple containing tile name and sensing time/date. Default is `None`
    :param folder: location of the directory where the fetched data will be saved. Default is ``'.'``
    :param redownload: if `True`, download again the requested data even though it's already saved to disk. If
        `False`, do not download if data is already available on disk. Default is `False`
    :param entire_product: in case tile is specified this flag determines if it will be place inside a .SAFE structure
        of the product. Default is `False`
    :param bands: list of bands to download. If `None` all bands will be downloaded. Default is `None`
    :param data_collection: In case of tile request the collection of satellite data has to be specified.
    :return: Nested dictionaries representing .SAFE structure.
    """
    safe_request: Union[None, AwsTileRequest, AwsProductRequest] = None
    entire_product = entire_product and product_id is None
    if tile is not None:
        if data_collection is None:
            raise ValueError("The data_collection parameter must be set.")
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
        if entire_product:
            if tile is None:
                raise ValueError("The tile parameter must be set.")
            safe_request = AwsProductRequest(
                product_id, tile_list=[tile[0]], data_folder=folder, bands=bands, safe_format=True
            )
        else:
            safe_request = AwsProductRequest(product_id, data_folder=folder, bands=bands, safe_format=True)

    if safe_request is None:
        raise ValueError("Either 'product_id' or 'tile' has to be defined")

    safe_request.save_data(redownload=redownload)

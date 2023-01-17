"""
Module for defining how satellite data will be collected from AWS and where it will be saved.
"""

import datetime as dt
import os
import warnings
from abc import ABCMeta, abstractmethod
from typing import Any, List, Optional, Tuple, Union

from ..api.opensearch import get_tile_info, get_tile_info_id
from ..config import SHConfig
from ..constants import MimeType
from ..data_collections import DataCollection
from ..download import DownloadRequest
from ..exceptions import AwsDownloadFailedException, SHUserWarning, deprecated_class
from ..time_utils import parse_time
from .client import AwsDownloadClient
from .constants import AwsConstants, EsaSafeType

MAX_SUPPORTED_BASELINES = {
    DataCollection.SENTINEL2_L1C: "04.00",
    DataCollection.SENTINEL2_L2A: "04.00",
}

REQUESTER_PAYS_PARAMS = {"RequestPayer": "requester"}


class AwsData(metaclass=ABCMeta):
    """A base class for collecting satellite data from AWS."""

    def __init__(
        self,
        parent_folder: str = "",
        bands: Union[None, str, List[str]] = None,
        metafiles: Union[None, str, List[str]] = None,
        config: Optional[SHConfig] = None,
    ):
        """
        :param parent_folder: Folder where the fetched data will be saved.
        :param bands: List of Sentinel-2 bands for request. If parameter is set to `None` all bands will be used.
        :param metafiles: List of additional metafiles available on AWS
                          (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``).
                          If parameter is set to `None` the list will be set automatically.
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        self.parent_folder = parent_folder
        self.bands = self._parse_bands(bands)
        self.metafiles = self._parse_metafiles(metafiles)
        self.config = config or SHConfig()

        self.download_list: List[DownloadRequest] = []
        self.folder_list: List[str] = []

        self.base_url = self.get_base_url()
        self.base_http_url = self.get_base_url(force_http=True)

        # These need to be set by the child classes
        self.baseline: str
        self.safe_type: EsaSafeType
        self.data_collection: DataCollection
        self.date: dt.date
        self.product_id: str

    @abstractmethod
    def get_requests(self) -> Tuple[List[DownloadRequest], List[str]]:
        """Abstract class for joining together download requests"""
        raise NotImplementedError

    def _parse_bands(self, band_input: Union[None, str, List[str]]) -> List[str]:
        """
        Parses class input and verifies band names.

        :param band_input: input parameter `bands`
        :return: verified list of bands
        """
        all_bands = (
            AwsConstants.S2_L1C_BANDS
            if self.data_collection is DataCollection.SENTINEL2_L1C
            else AwsConstants.S2_L2A_BANDS
        )

        if band_input is None:
            return all_bands
        if isinstance(band_input, str):
            band_list = band_input.split(",")
        elif isinstance(band_input, list):
            band_list = band_input.copy()
        else:
            raise ValueError("bands parameter must be a list or a string")
        band_list = [band.strip().split(".")[0] for band in band_list]
        band_list = [band for band in band_list if band != ""]
        if not set(band_list) <= set(all_bands):
            raise ValueError(f"bands {band_list} must be a subset of {all_bands}")
        return band_list

    def _parse_metafiles(self, metafile_input: Union[None, str, List[str]]) -> List[str]:
        """Parses class input and verifies metadata file names.

        :param metafile_input: class input parameter `metafiles`
        :return: verified list of metadata files
        """
        all_metafiles = (
            AwsConstants.S2_L1C_METAFILES
            if self.data_collection is DataCollection.SENTINEL2_L1C
            else AwsConstants.S2_L2A_METAFILES
        )

        if metafile_input is None:
            if self.__class__.__name__ == "SafeProduct":
                return all_metafiles
            if self.__class__.__name__ == "SafeTile":
                return [metafile for metafile in all_metafiles if metafile in AwsConstants.TILE_FILES]
            return []
        if isinstance(metafile_input, str):
            metafile_list = metafile_input.split(",")
        elif isinstance(metafile_input, list):
            metafile_list = metafile_input.copy()
        else:
            raise ValueError("metafiles parameter must be a list or a string")
        metafile_list = [metafile.strip().split(".")[0] for metafile in metafile_list]
        metafile_list = [metafile for metafile in metafile_list if metafile != ""]
        if not set(metafile_list) <= set(all_metafiles):
            raise ValueError(f"metadata files {metafile_list} must be a subset of {all_metafiles}")
        return metafile_list

    def get_base_url(self, force_http: bool = False) -> str:
        """Creates base URL path

        :param force_http: `True` if HTTP base URL should be used and `False` otherwise
        :return: base url string
        """
        base_url = self.config.aws_metadata_url if force_http else "s3://"
        aws_bucket = (
            self.config.aws_s3_l1c_bucket
            if self.data_collection is DataCollection.SENTINEL2_L1C
            else self.config.aws_s3_l2a_bucket
        )

        return base_url + ("" if base_url.endswith("/") else "/") + aws_bucket

    def get_safe_type(self) -> EsaSafeType:
        """Determines the type of ESA product.

        In 2016 ESA changed structure and naming of data. Therefore, the class must
        distinguish between old product type and compact (new) product type.

        :return: type of ESA product
        :raises: ValueError
        """
        product_type = self.product_id.split("_")[1]
        if product_type.startswith("MSI"):
            return EsaSafeType.COMPACT_TYPE
        if product_type in ["OPER", "USER"]:
            return EsaSafeType.OLD_TYPE
        raise ValueError(f"Unrecognized product type of product id {self.product_id}")

    def get_baseline(self) -> str:
        """Determines the baseline number (i.e. version) of ESA .SAFE product

        :return: baseline number
        :raises: ValueError
        """
        if self.get_safe_type() is EsaSafeType.COMPACT_TYPE:
            baseline = self.product_id.split("_")[3].lstrip("N")
            if len(baseline) != 4:
                raise ValueError(f"Unable to recognize baseline number from the product id {self.product_id}")
            baseline = f"{baseline[:2]}.{baseline[2:]}"

            if baseline > MAX_SUPPORTED_BASELINES[self.data_collection]:
                message = (
                    f"Products with baseline {baseline} are not officially supported in sentinelhub-py. If you notice "
                    "any errors in naming structure of downloaded data please report an issue at "
                    "https://github.com/sentinel-hub/sentinelhub-py/issues. Pull requests are also very appreciated"
                )
                warnings.warn(message, category=SHUserWarning)

            return baseline
        return self._read_baseline_from_info()

    def _read_baseline_from_info(self) -> str:
        """Tries to find and return baseline number from either tileInfo or productInfo file.

        :return: Baseline ID
        :raises: ValueError
        """
        if hasattr(self, "tile_info"):
            return self.tile_info["datastrip"]["id"][-5:]
        if hasattr(self, "product_info"):
            return self.product_info["datastrips"][0]["id"][-5:]
        raise ValueError("No info file has been obtained yet.")

    @staticmethod
    def url_to_tile(url: str) -> Tuple[str, str, int]:
        """Extracts tile name, date and AWS index from tile url on AWS.

        :param url: class input parameter 'metafiles'
        :return: Name of tile, date and AWS index which uniquely identifies tile on AWS
        """
        info = url.strip("/").split("/")
        name = "".join(info[-7:-4])
        date = "-".join(info[-4:-1])
        return name, date, int(info[-1])

    def sort_download_list(self) -> None:
        """Method for sorting the list of download requests. Band images have priority before metadata files. If bands
        images or metadata files are specified with a list they will be sorted in the same order as in the list.
        Otherwise, they will be sorted alphabetically (band B8A will be between B08 and B09).
        """

        def aws_sort_function(download_request: DownloadRequest) -> Tuple[int, str, int]:
            data_name = download_request.extra_params["data_name"]
            if "product_name" in download_request.extra_params:
                product_name = download_request.extra_params["product_name"]
            else:
                if download_request.url is None:
                    raise ValueError(f"Faulty request {download_request}, no URL specified.")
                product_name = self._url_to_props(download_request.url)[0]
            if data_name in self.bands:
                return 0, product_name, self.bands.index(data_name)
            return 1, product_name, self.metafiles.index(data_name)

        self.download_list.sort(key=aws_sort_function)

    def structure_recursion(self, struct: dict, folder: str) -> None:
        """From nested dictionaries representing .SAFE structure it recursively extracts all the files that need to be
        downloaded and stores them into class attribute `download_list`.

        :param struct: nested dictionaries representing a part of .SAFE structure
        :param folder: name of folder where this structure will be saved
        """
        has_subfolder = False
        for name, substruct in struct.items():
            subfolder = os.path.join(folder, name)
            if not isinstance(substruct, dict):
                product_name, data_name = self._url_to_props(substruct)
                if "." in data_name:
                    data_type = MimeType(data_name.split(".", 1)[-1])
                    data_name = data_name.rsplit(".", 1)[0]
                else:
                    data_type = MimeType.RAW
                if data_name in self.bands + self.metafiles:
                    self.download_list.append(
                        DownloadRequest(
                            url=substruct,
                            filename=subfolder,
                            data_type=data_type,
                            extra_params={
                                "data_name": data_name,
                                "product_name": product_name,
                            },
                        )
                    )
            else:
                has_subfolder = True
                self.structure_recursion(substruct, subfolder)
        if not has_subfolder:
            self.folder_list.append(folder)

    def _url_to_props(self, url: str) -> Tuple[str, str]:
        """Converts url back to name of product/tile and name of the file

        :param url: URL location of the data
        :return: Names of product or tile and name of a file
        """
        props = (
            (url[len(self.base_url) :] if url.startswith(self.base_url) else url[len(self.base_http_url) :])
            .lstrip("/")
            .split("/")
        )
        if props[0] == "products":
            tile_props = props[:5]
            props = props[5:]
            if props[0] == "datastrip":
                props[1] = "*"
        else:
            tile_props = props[:8]
            props = props[8:]
        return "/".join(tile_props), "/".join(props)

    @staticmethod
    def add_file_extension(filename: str, data_format: Optional[MimeType] = None, remove_path: bool = False) -> str:
        """Joins filename and corresponding file extension if it has one.

        :param filename: Name of the file without extension
        :param data_format: format of file, if `None` it will be set automatically
        :param remove_path: `True` if the path in filename string should be removed
        :return: Name of the file with extension
        """
        if data_format is None:
            data_format = AwsConstants.AWS_FILES[filename]
        if remove_path:
            filename = filename.split("/")[-1]
        if filename.startswith("datastrip"):
            filename = filename.replace("*", "0")
        if data_format is MimeType.RAW:
            return filename
        return f"{filename.replace('*', '')}.{data_format.value}"

    def has_reports(self) -> bool:
        """Products created with baseline 2.06 and greater (and some products with baseline 2.05) should have quality
        report files

        :return: `True` if the product has report xml files and `False` otherwise
        """
        # Not completely sure if this condition is correct
        return self.baseline > "02.05" or (
            self.baseline == "02.05" and self.date >= dt.date(year=2017, month=10, day=12)
        )

    def is_early_compact_l2a(self) -> bool:
        """Check if product is early version of compact L2A product

        :return: `True` if product is early version of compact L2A product and `False` otherwise
        """
        return (
            self.data_collection is DataCollection.SENTINEL2_L2A
            and self.safe_type is EsaSafeType.COMPACT_TYPE
            and "00.01" < self.baseline <= "02.06"
        )


@deprecated_class(message_suffix="It will remain in the codebase for now, but won't be actively maintained.")
class AwsProduct(AwsData):
    """Class for collecting Sentinel-2 products data from AWS."""

    def __init__(self, product_id: str, tile_list: Union[None, str, List[str]] = None, **kwargs: Any):
        """
        :param product_id: ESA ID of the product
        :param tile_list: list of tile names
        :param parent_folder: location of the directory where the fetched data will be saved.
        :param bands: List of Sentinel-2 bands for request. If parameter is set to `None` all bands will be used.
        :param metafiles: List of additional metafiles available on AWS
                          (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``).
                          If parameter is set to `None` the list will be set automatically.
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        self.product_id = product_id.split(".")[0]
        self.tile_list = self.parse_tile_list(tile_list)

        self.data_collection = self.get_data_collection()
        self.safe_type = self.get_safe_type()

        super().__init__(**kwargs)

        self.date = self.get_date()
        self.product_url = self.get_product_url()

        client = AwsDownloadClient(config=self.config, boto_params=REQUESTER_PAYS_PARAMS)
        self.product_info = client.get_json_dict(self.get_url(AwsConstants.PRODUCT_INFO))
        self.baseline = self.get_baseline()

    @staticmethod
    def parse_tile_list(tile_input: Union[None, str, List[str]]) -> Optional[List[str]]:
        """Parses class input and verifies band names.

        :param tile_input: class input parameter `tile_list`
        :return: parsed list of tiles
        """
        if tile_input is None:
            return None
        if isinstance(tile_input, str):
            tile_list = tile_input.split(",")
        elif isinstance(tile_input, list):
            tile_list = tile_input.copy()
        else:
            raise ValueError("tile_list parameter must be a list of tile names")
        tile_list = [AwsTile.parse_tile_name(tile_name) for tile_name in tile_list]
        return tile_list

    def get_requests(self) -> Tuple[List[DownloadRequest], List[str]]:
        """Creates product structure and returns list of files for download.

        :return: List of download requests and list of empty folders that need to be created
        """
        self.download_list = [
            DownloadRequest(
                url=self.get_url(metafile),
                filename=self.get_filepath(metafile),
                data_type=AwsConstants.AWS_FILES[metafile],
                extra_params={"data_name": metafile},
            )
            for metafile in self.metafiles
            if metafile in AwsConstants.PRODUCT_FILES
        ]

        tile_parent_folder = os.path.join(self.parent_folder, self.product_id)
        for tile_info in self.product_info["tiles"]:
            tile_name, date, aws_index = self.url_to_tile(self.get_tile_url(tile_info))
            if self.tile_list is None or AwsTile.parse_tile_name(tile_name) in self.tile_list:
                tile_downloads, tile_folders = AwsTile(
                    tile_name,
                    date,
                    aws_index,
                    parent_folder=tile_parent_folder,
                    bands=self.bands,
                    metafiles=self.metafiles,
                    data_collection=self.data_collection,
                ).get_requests()
                self.download_list.extend(tile_downloads)
                self.folder_list.extend(tile_folders)
        self.sort_download_list()
        return self.download_list, self.folder_list

    def get_data_collection(self) -> DataCollection:
        """The method determines data collection from product ID.

        :return: Data collection of the product
        :raises: ValueError
        """
        product_type = self.product_id.split("_")[1]
        if product_type.endswith("L1C") or product_type == "OPER":
            return DataCollection.SENTINEL2_L1C
        if product_type.endswith("L2A") or product_type == "USER":
            return DataCollection.SENTINEL2_L2A
        raise ValueError(f"Unknown data collection of product {self.product_id}")

    def get_date(self) -> dt.date:
        """Collects sensing date of the product.

        :return: Sensing date
        """
        if self.safe_type is EsaSafeType.OLD_TYPE:
            name = self.product_id.split("_")[-2]
            date = (name[1:5], name[5:7], name[7:9])
        else:
            name = self.product_id.split("_")[2]
            date = (name[:4], name[4:6], name[6:8])

        return dt.date(year=int(date[0]), month=int(date[1]), day=int(date[2]))

    def get_url(self, filename: str, data_format: Optional[MimeType] = None) -> str:
        """Creates url of file location on AWS.

        :param filename: name of file
        :param data_format: format of file, if `None` it will be set automatically
        :return: url of file location
        """
        product_url = self.product_url
        force_http = filename in [AwsConstants.PRODUCT_INFO, AwsConstants.METADATA]
        if product_url is None or force_http:
            product_url = self.get_product_url(force_http=force_http)
        return f"{product_url}/{self.add_file_extension(filename, data_format)}"

    def get_product_url(self, force_http: bool = False) -> str:
        """Creates base url of product location on AWS.

        :param force_http: `True` if HTTP base URL should be used and `False` otherwise
        :return: url of product location
        """
        base_url = self.base_http_url if force_http else self.base_url
        return f"{base_url}/products/{self.date.year}/{self.date.month}/{self.date.day}/{self.product_id}"

    def get_tile_url(self, tile_info: dict) -> str:
        """Collects tile url from `productInfo.json` file.

        :param tile_info: information about tile from `productInfo.json`
        :return: url of tile location
        """
        return f"{self.base_url}/{tile_info['path']}"

    def get_filepath(self, filename: str) -> str:
        """Creates file path for the file.

        :param filename: name of the file
        :return: filename with path on disk
        """
        return os.path.join(self.parent_folder, self.product_id, self.add_file_extension(filename)).replace(":", ".")


@deprecated_class(message_suffix="It will remain in the codebase for now, but won't be actively maintained.")
class AwsTile(AwsData):
    """Class for collecting Sentinel-2 tiles data from AWS."""

    def __init__(
        self,
        tile_name: str,
        time: str,
        aws_index: Optional[int] = None,
        data_collection: DataCollection = DataCollection.SENTINEL2_L1C,
        **kwargs: Any,
    ):
        """
        :param tile: Tile name (e.g. 'T10UEV')
        :param time: Tile sensing time in ISO8601 format
        :param aws_index: There exist Sentinel-2 tiles with the same tile and time parameter. Therefore, each tile
            on AWS also has an index which is visible in their url path. If `aws_index` is set to `None` the
            class will try to find the index automatically. If there will be multiple choices it will choose
            the lowest index and inform the user.
        :param data_collection: A collection of requested AWS data. Supported collections are Sentinel-2 L1C and
            Sentinel-2 L2A, default is Sentinel-2 L1C data.
        :param parent_folder: folder where the fetched data will be saved.
        :param bands: List of Sentinel-2 bands for request. If parameter is set to `None` all bands will be used.
        :param metafiles: List of additional metafiles available on AWS
                          (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``).
                          If parameter is set to `None` the list will be set automatically.
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        self.tile_name = self.parse_tile_name(tile_name)

        self.timestamp: dt.date = parse_time(time, ignoretz=True)
        self.date = self.timestamp.date() if isinstance(self.timestamp, dt.datetime) else self.timestamp

        self.aws_index = aws_index
        self.data_collection = data_collection

        super().__init__(**kwargs)
        self.tile_url = None

        self.aws_index = self.get_aws_index()
        self.tile_url = self.get_tile_url()
        self.tile_info = self.get_tile_info()
        if not self.tile_is_valid():
            raise ValueError("Cannot find data on AWS for specified tile, time and aws_index")

        self.product_id = self.get_product_id()
        self.safe_type = self.get_safe_type()
        self.baseline = self.get_baseline()

    @staticmethod
    def parse_tile_name(name: str) -> str:
        """
        Parses and verifies tile name.

        :param name: class input parameter `tile_name`
        :return: parsed tile name
        """
        tile_name = name.lstrip("T0")
        if len(tile_name) == 4:
            tile_name = "0" + tile_name
        if len(tile_name) != 5:
            raise ValueError(f"Invalid tile name {name}")
        return tile_name

    def get_requests(self) -> Tuple[List[DownloadRequest], List[str]]:
        """
        Creates tile structure and returns list of files for download.

        :return: List of download requests and list of empty folders that need to be created
        """
        self.download_list = []
        for data_name in [band for band in self.bands if self._band_exists(band)] + self.metafiles:
            if data_name in AwsConstants.TILE_FILES:
                url = self.get_url(data_name)
                filename = self.get_filepath(data_name)
                self.download_list.append(
                    DownloadRequest(
                        url=url,
                        filename=filename,
                        data_type=AwsConstants.AWS_FILES[data_name],
                        extra_params={"data_name": data_name},
                    )
                )
        self.sort_download_list()
        return self.download_list, self.folder_list

    def get_aws_index(self) -> int:
        """
        Returns tile index on AWS. If `tile_index` was not set during class initialization it will be determined
        according to existing tiles on AWS.

        :return: Index of tile on AWS
        """
        if self.aws_index is not None:
            return self.aws_index
        tile_info_list = get_tile_info(self.tile_name, self.timestamp, all_tiles=True)
        if not tile_info_list:
            raise ValueError("Cannot find aws_index for specified tile and time")

        if self.data_collection is DataCollection.SENTINEL2_L2A:
            for tile_info in sorted(tile_info_list, key=self._parse_aws_index):  # type: ignore
                try:
                    self.aws_index = self._parse_aws_index(tile_info)  # type: ignore
                    self.get_tile_info()
                    return self.aws_index
                except AwsDownloadFailedException:
                    pass

        return self._parse_aws_index(tile_info_list[0])  # type: ignore

    @staticmethod
    def _parse_aws_index(tile_info: dict) -> int:
        """Parses an AWS index from tile info

        :param tile_info: dictionary with information about tile
        :return: Index of tile on AWS
        """
        return int(tile_info["properties"]["s3Path"].split("/")[-1])

    def tile_is_valid(self) -> bool:
        """Checks if tile has tile info and valid timestamp

        :return: `True` if tile is valid and `False` otherwise
        """
        tile_info_datetime = parse_time(self.tile_info["timestamp"], ignoretz=True) if self.tile_info else None
        only_date_given = not isinstance(self.timestamp, dt.datetime)
        return self.tile_info is not None and (only_date_given or self.timestamp == tile_info_datetime)

    def get_tile_info(self) -> dict:
        """
        Collects basic info about tile from tileInfo.json.

        :return: dictionary with tile information
        """
        client = AwsDownloadClient(config=self.config, boto_params=REQUESTER_PAYS_PARAMS)
        return client.get_json_dict(self.get_url(AwsConstants.TILE_INFO))

    def get_url(self, filename: str) -> str:
        """
        Creates url of file location on AWS.

        :param filename: name of file
        :return: url of file location
        """
        tile_url = self.tile_url
        force_http = filename in [AwsConstants.TILE_INFO, AwsConstants.PRODUCT_INFO, AwsConstants.METADATA]
        if tile_url is None or force_http:
            tile_url = self.get_tile_url(force_http=force_http)
        return f"{tile_url}/{self.add_file_extension(filename)}"

    def get_tile_url(self, force_http: bool = False) -> str:
        """
        Creates base url of tile location on AWS.

        :param force_http: `True` if HTTP base URL should be used and `False` otherwise
        :return: url of tile location
        """
        base_url = self.base_http_url if force_http else self.base_url
        name_parts = self.tile_name[0:2].lstrip("0"), self.tile_name[2], self.tile_name[3:5]

        return (
            f"{base_url}/tiles/{name_parts[0]}/{name_parts[1]}/{name_parts[2]}/{self.date.year}/"
            f"{self.date.month}/{self.date.day}/{self.aws_index}"
        )

    def get_qi_url(self, metafile: str) -> str:
        """Returns url of tile metadata products

        :param metafile: Name of metadata product at AWS
        :return: url location of metadata product at AWS
        """
        return f"{self.tile_url}/qi/{metafile}"

    def get_band_qi_url(self, qi_type: str, band: str = "B00", data_format: MimeType = MimeType.GML) -> str:
        """
        :param qi_type: type of quality indicator
        :param band: band name
        :return: location of gml file on AWS
        """
        band = band.split("/")[-1]
        if data_format == MimeType.JP2:
            return self.get_qi_url(f"{qi_type}_{band}.{data_format.value}")
        return self.get_qi_url(f"MSK_{qi_type}_{band}.{data_format.value}")

    def get_preview_url(self, data_type: str = "L1C") -> str:
        """Returns url location of full resolution L1C preview"""
        if self.data_collection is DataCollection.SENTINEL2_L1C or self.safe_type is EsaSafeType.OLD_TYPE:
            return self.get_url(AwsConstants.PREVIEW_JP2)
        return self.get_qi_url(f"{data_type}_PVI.jp2")

    def get_filepath(self, filename: str) -> str:
        """
        Creates file path for the file.

        :param filename: name of the file
        :return: filename with path on disk
        """
        tile_folder = f"{self.tile_name},{self.date.isoformat()},{self.aws_index}"
        return os.path.join(self.parent_folder, tile_folder, self.add_file_extension(filename)).replace(":", ".")

    def get_product_id(self) -> str:
        """
        Obtains ESA ID of product which contains the tile.

        :return: ESA ID of the product
        """
        return self.tile_info["productName"]

    def _band_exists(self, band_name: str) -> bool:
        if self.data_collection is DataCollection.SENTINEL2_L1C:
            return True
        resolution, band = band_name.split("/")

        if band == "B01" and resolution == AwsConstants.R20m and self.baseline < "04.00":
            return False

        if self.safe_type is EsaSafeType.COMPACT_TYPE:
            return not (band == AwsConstants.VIS and (self.baseline >= "02.07" or self.baseline == "00.01"))

        return band != AwsConstants.TCI and not (band == AwsConstants.SCL and resolution == AwsConstants.R60m)

    @staticmethod
    def tile_id_to_tile(tile_id: str) -> Tuple[str, str, int]:
        """
        :param tile_id: original tile identification string provided by ESA (e.g.
                        'S2A_OPER_MSI_L1C_TL_SGS__20160109T230542_A002870_T10UEV_N02.01')
        :return: tile name, sensing date and AWS index
        """
        if tile_id.split("_")[0] not in ["S2A", "S2B", "L1C"]:
            raise ValueError("Transformation from tile ID to tile works currently only for Sentinel-2 L1C products")

        tile_info = get_tile_info_id(tile_id)
        return AwsData.url_to_tile(tile_info["properties"]["s3Path"])

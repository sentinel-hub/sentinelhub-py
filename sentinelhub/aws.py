"""
Module for obtaining data from Amazon Web Service
"""

from abc import ABC, abstractmethod
import logging
import os.path

from .download import DownloadRequest, get_json
from .opensearch import get_tile_info, get_tile_info_id
from .time_utils import parse_time
from .config import SGConfig
from .constants import AwsConstants, EsaSafeType, MimeType


LOGGER = logging.getLogger(__name__)


class AwsService(ABC):
    """ Amazon Web Service (AWS) base class

    :param parent_folder: Folder where the fetched data will be saved.
    :type parent_folder: str
    :param bands: List of Sentinel-2 bands for request. If parameter is set to ``None`` all bands will be used.
    :type bands: list(str) or None
    :param metafiles: List of additional metafiles available on AWS
                      (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``).
                      If parameter is set to ``None`` the list will be set automatically.
    :type metafiles: list(str) or None
    """
    def __init__(self, parent_folder='', bands=None, metafiles=None):
        self.parent_folder = parent_folder
        self.bands = self.parse_bands(bands)
        self.metafiles = self.parse_metafiles(metafiles)

        self.base_url = SGConfig().aws_base_url
        self.download_list = []
        self.folder_list = []

    @abstractmethod
    def get_requests(self):
        raise NotImplementedError

    @staticmethod
    def parse_bands(band_input):
        """
        Parses class input and verifies band names.

        :param band_input: input parameter `bands`
        :type band_input: str or list(str)
        :return: verified list of bands
        :rtype: list(str)
        """
        if band_input is None:
            return AwsConstants.BANDS
        if isinstance(band_input, str):
            band_list = band_input.split(',')
        elif isinstance(band_input, list):
            band_list = band_input.copy()
        else:
            raise ValueError('bands parameter must be a list or a string')
        band_list = [band.strip().split('.')[0] for band in band_list]
        band_list = [band for band in band_list if band != '']
        if not set(band_list) <= set(AwsConstants.BANDS):
            raise ValueError('bands must be a subset of {}'.format(AwsConstants.BANDS))
        return band_list

    def parse_metafiles(self, metafile_input):
        """
        Parses class input and verifies metadata file names.

        :param metafile_input: class input parameter `metafiles`
        :type metafile_input: str or list(str)
        :return: verified list of metadata files
        :rtype: list(str)
        """
        if metafile_input is None:
            if self.__class__.__name__ == 'SafeProduct' or self.__class__.__name__ == 'SafeTile':
                return sorted(AwsConstants.FILE_FORMATS.keys())
            return []
        if isinstance(metafile_input, str):
            metafile_list = metafile_input.split(',')
        elif isinstance(metafile_input, list):
            metafile_list = metafile_input.copy()
        else:
            raise ValueError('metafiles parameter must be a list or a string')
        metafile_list = [metafile.strip().split('.')[0] for metafile in metafile_list]
        metafile_list = [metafile for metafile in metafile_list if metafile != '']
        if not set(metafile_list) <= set(AwsConstants.FILE_FORMATS.keys()):
            raise ValueError('metafiles must be a subset of {}'.format(
                list(AwsConstants.FILE_FORMATS.keys())))
        return metafile_list

    @staticmethod
    def url_to_tile(url):
        """
        Extracts tile name, date and AWS index from tile url on AWS.

        :param url: class input parameter 'metafiles'
        :type url: str
        :return: Name of tile, date and AWS index which uniquely identifies tile on AWS
        :rtype: (str, str, int)
        """
        info = url.strip('/').split('/')
        name = ''.join(info[-7: -4])
        date = '-'.join(info[-4: -1])
        return name, date, int(info[-1])

    def sort_download_list(self):
        """
        Method for sorting the list of download requests. Band images have priority before metadata files. If bands
        images or metadata files are specified with a list they will be sorted in the same order as in the list.
        Otherwise they will be sorted alphabetically (band B8A will be between B08 and B09).
        """
        def aws_sort_function(download_request):
            data_name = download_request.properties['data_name']
            tile_url = download_request.url.rsplit('.', 1)[0].rstrip(data_name).rstrip('/')
            if data_name in AwsConstants.BANDS:
                return 0, tile_url, self.bands.index(data_name)
            return 1, tile_url, self.metafiles.index(data_name)
        self.download_list.sort(key=aws_sort_function)

    def structure_recursion(self, struct, folder):
        """
        From nested dictionaries representing .SAFE structure it recursively extracts all the files that need to be
        downloaded and stores them into class attribute `download_list`.

        :param struct: nested dictionaries representing a part of .SAFE structure
        :type struct: dict
        :param folder: name of folder where this structure will be saved
        :type folder: str
        """
        if not struct:
            self.folder_list.append(folder)
            return
        for name, substruct in struct.items():
            subfolder = os.path.join(folder, name)
            if not isinstance(substruct, dict):
                if substruct.split('/')[3] == 'products':
                    data_name = substruct.split('/', 8)[-1]
                    if '/' in data_name:
                        items = data_name.split('/')
                        data_name = '/'.join([items[0], '*', items[2]])
                else:
                    data_name = substruct.split('/', 11)[-1]
                if '.' in data_name:
                    data_type = MimeType(substruct.split('.')[-1])
                    data_name = data_name.rsplit('.', 1)[0]
                else:
                    data_type = MimeType.RAW
                if data_name in self.bands + self.metafiles:
                    self.download_list.append(DownloadRequest(url=substruct, filename=subfolder, data_type=data_type,
                                                              data_name=data_name))
            else:
                self.structure_recursion(substruct, subfolder)

    @staticmethod
    def add_filename_extension(filename):
        """
        Joins filename and corresponding file extension if it has one.

        :param filename: Name of the file without extension
        :type filename: str
        :return: Name of the file with extension
        :rtype: str
        """
        if AwsConstants.FILE_FORMATS[filename] is MimeType.RAW:
            return filename
        return '{}.{}'.format(filename, AwsConstants.FILE_FORMATS[filename].value)


class AwsProduct(AwsService):
    """ Service class for Sentinel-2 product on AWS

    :param product_id: ESA ID of the product
    :type product_id: str
    :param tile_list: list of tile names
    :type tile_list: list(str) or None
    :param parent_folder: location of the directory where the fetched data will be saved.
    :type parent_folder: str
    :param bands: List of Sentinel-2 bands for request. If parameter is set to ``None`` all bands will be used.
    :type bands: list(str) or None
    :param metafiles: List of additional metafiles available on AWS
                      (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``).
                      If parameter is set to ``None`` the list will be set automatically.
    :type metafiles: list(str) or None
    """
    def __init__(self, product_id, tile_list=None, **kwargs):
        super(AwsProduct, self).__init__(**kwargs)

        self.product_id = product_id.split('.')[0]
        self.tile_list = self.parse_tile_list(tile_list)

        self.safe_type = self.get_safe_type()
        self.date = self.get_date()
        self.product_url = self.get_product_url()
        self.product_info = get_json(self.get_url(AwsConstants.PRODUCT_INFO))

    @staticmethod
    def parse_tile_list(tile_input):
        """
        Parses class input and verifies band names.

        :param tile_input: class input parameter `tile_list`
        :type tile_input: str or list(str)
        :return: parsed list of tiles
        :rtype: list(str) or None
        """
        if tile_input is None:
            return None
        if isinstance(tile_input, str):
            tile_list = tile_input.split(',')
        elif isinstance(tile_input, list):
            tile_list = tile_input.copy()
        else:
            raise ValueError('tile_list parameter must be a list of tile names')
        tile_list = [AwsTile.parse_tile_name(tile_name) for tile_name in tile_list]
        return tile_list

    def get_requests(self):
        """
        Creates product structure and returns list of files for download.

        :return: List of download requests and list of empty folders that need to be created
        :rtype: (list(download.DownloadRequest), list(str))
        """
        self.download_list = [DownloadRequest(url=self.get_url(metafile), filename=self.get_filepath(metafile),
                                              data_type=AwsConstants.FILE_FORMATS[metafile], data_name=metafile) for
                              metafile in self.metafiles if metafile in AwsConstants.PRODUCT_METAFILES]

        tile_parent_folder = os.path.join(self.parent_folder, self.product_id)
        for tile_info in self.product_info['tiles']:
            tile_name, date, aws_index = self.url_to_tile(self.get_tile_url(tile_info))
            if self.tile_list is None or AwsTile.parse_tile_name(tile_name) in self.tile_list:
                tile_downloads, tile_folders = AwsTile(tile_name, date, aws_index, parent_folder=tile_parent_folder,
                                                       bands=self.bands, metafiles=self.metafiles).get_requests()
                self.download_list.extend(tile_downloads)
                self.folder_list.extend(tile_folders)
        self.sort_download_list()
        return self.download_list, self.folder_list

    def get_safe_type(self):
        """ Determines the type of ESA product.

        In 2016 ESA changed structure and naming of data. Therefore the class must
        distinguish between old product type and compact (new) product type.

        :return: type of ESA product
        :rtype: constants.EsaSafeType
        """
        if self.product_id.split('_')[1] == 'MSIL1C':
            return EsaSafeType.COMPACT_SAFE_TYPE
        return EsaSafeType.OLD_SAFE_TYPE

    def get_date(self):
        """ Collects sensing date of the product.

        :return: Sensing date
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_SAFE_TYPE:
            name = self.product_id.split('_')[-2]
            date = [name[1:5], name[5:7], name[7:9]]
        elif self.safe_type == EsaSafeType.COMPACT_SAFE_TYPE:
            name = self.product_id.split('_')[2]
            date = [name[:4], name[4:6], name[6:8]]
        return '-'.join(date_part.lstrip('0') for date_part in date)

    def get_url(self, filename):
        """
        Creates url of file location on AWS.

        :param filename: name of file
        :type filename: str
        :return: url of file location
        :rtype: str
        """
        if self.product_url is None:
            self.product_url = self.get_product_url()
        return '{}/{}'.format(self.product_url, self.add_filename_extension(filename))

    def get_product_url(self):
        """
        Creates base url of product location on AWS.

        :return: url of product location
        :rtype: str
        """
        return self.base_url + 'products/' + self.date.replace('-', '/') + '/' + self.product_id

    def get_tile_url(self, tile_info):
        """
        Collects tile url from productInfo.json file.

        :param tile_info: information about tile from productInfo.json
        :type tile_info: dict
        :return: url of tile location
        :rtype: str
        """
        return self.base_url + '/' + tile_info['path']

    def get_filepath(self, filename):
        """
        Creates file path for the file.

        :param filename: name of the file
        :type filename: str
        :return: filename with path on disk
        :rtype: str
        """
        return os.path.join(self.parent_folder, self.product_id,
                            self.add_filename_extension(filename)).replace(':', '.')


class AwsTile(AwsService):
    """ Service class for Sentinel-2 product on AWS

    :param tile: Tile name (e.g. 'T10UEV')
    :type tile: str
    :param time: Tile sensing time in ISO8601 format
    :type time: str
    :param aws_index: There exist Sentinel-2 tiles with the same tile and time parameter. Therefore each tile on AWS
                      also has an index which is visible in their url path. If ``aws_index`` is set to ``None`` the
                      class will try to find the index automatically. If there will be multiple choices it will choose
                      the lowest index and inform the user.
    :type aws_index: int or None
    :param parent_folder: folder where the fetched data will be saved.
    :type parent_folder: str
    :param bands: List of Sentinel-2 bands for request. If parameter is set to ``None`` all bands will be used.
    :type bands: list(str) or None
    :param metafiles: List of additional metafiles available on AWS
                      (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``).
                      If parameter is set to ``None`` the list will be set automatically.
    :type metafiles: list(str) or None
    """
    def __init__(self, tile_name, time, aws_index=None, **kwargs):
        super(AwsTile, self).__init__(**kwargs)

        self.tile_name = self.parse_tile_name(tile_name)
        self.datetime = self.parse_datetime(time)
        self.date = self.datetime.split('T')[0]
        self.aws_index = aws_index

        LOGGER.debug('tile_name=%s, date=%s, bands=%s, metafiles=%s', self.tile_name, self.date,
                     self.bands, self.metafiles)

        self.aws_index = self.get_aws_index()
        self.tile_url = self.get_tile_url()
        self.tile_info = self.get_tile_info()
        if not self.tile_is_valid():
            raise ValueError('Cannot find data on AWS for specified tile, time and aws_index')

    @staticmethod
    def parse_tile_name(name):
        """
        Parses and verifies tile name.

        :param name: class input parameter `tile_name`
        :type name: str
        :return: parsed tile name
        :rtype: str
        """
        tile_name = name.lstrip('T0')
        if len(tile_name) == 4:
            tile_name = '0' + tile_name
        if len(tile_name) != 5:
            raise ValueError('Invalid tile name {}'.format(name))
        return tile_name

    @staticmethod
    def parse_datetime(time):
        """
        Parses and verifies tile sensing time.

        :param time: tile sensing time
        :type time: str
        :return: tile sensing time in ISO8601 format
        :rtype: str
        """
        try:
            return parse_time(time)
        except Exception:
            raise ValueError('Time must be in format YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS')

    def get_requests(self):
        """
        Creates tile structure and returns list of files for download.

        :return: List of download requests and list of empty folders that need to be created
        :rtype: (list(download.DownloadRequest), list(str))
        """
        self.download_list = []
        for data_name in self.bands + self.metafiles:
            if data_name in AwsConstants.TILE_FILES:
                url = self.get_url(data_name)
                filename = self.get_filepath(data_name)
                self.download_list.append(DownloadRequest(url=url, filename=filename,
                                                          data_type=AwsConstants.FILE_FORMATS[data_name],
                                                          data_name=data_name))
        self.sort_download_list()
        return self.download_list, self.folder_list

    def get_aws_index(self):
        """
        Returns tile index on AWS. If `tile_index` was not set during class initialization it will be determined
        according to existing tiles on AWS.

        :return: Index of tile on AWS
        :rtype: int
        """
        if self.aws_index is not None:
            return self.aws_index
        tile_info = get_tile_info(self.tile_name, self.datetime)
        if tile_info is not None:
            return int(tile_info['properties']['s3Path'].split('/')[-1])
        raise ValueError('Cannot find aws_index for specified tile and time')

    def tile_is_valid(self):
        return self.tile_info is not None \
               and (self.datetime == self.date or self.datetime == self.parse_datetime(self.tile_info['timestamp']))

    def get_tile_info(self):
        """
        Collects basic info about tile from tileInfo.json.

        :return: dictionary with tile information
        :rtype: dict
        """
        url = self.get_url('tileInfo')
        try:
            return get_json(url)
        except Exception as err:
            LOGGER.error('Download from url %s failed with %s', url, err)
            raise

    def get_url(self, filename):
        """
        Creates url of file location on AWS.

        :param filename: name of file
        :type filename: str
        :return: url of file location
        :rtype: str
        """
        if self.tile_url is None or filename == AwsConstants.TILE_INFO:
            self.tile_url = self.get_tile_url()
        return '{}/{}'.format(self.tile_url, self.add_filename_extension(filename))

    def get_tile_url(self):
        """
        Creates base url of tile location on AWS.

        :return: url of tile location
        :rtype: str
        """
        url = self.base_url + 'tiles/' + self.tile_name[0:2].lstrip('0') + '/' + self.tile_name[2] + '/' \
            + self.tile_name[3:5] + '/'
        date_params = self.date.split('-')
        for param in date_params:
            url += param.lstrip('0') + '/'
        return url + str(self.aws_index)

    def get_filepath(self, filename):
        """
        Creates file path for the file.

        :param filename: name of the file
        :type filename: str
        :return: filename with path on disk
        :rtype: str
        """
        return os.path.join(self.parent_folder, '{},{},{}'.format(self.tile_name, self.date, self.aws_index),
                            self.add_filename_extension(filename)).replace(':', '.')

    def get_product_id(self):
        """
        Obtains ESA ID of product which contains the tile.

        :return: ESA ID of the product
        :rtype: str
        """
        return self.tile_info['productName']

    @staticmethod
    def tile_id_to_tile(tile_id):
        """
        :param tile_id: original ESA tile ID
        :type: str
        :return: tile name, sensing date and AWS index
        :rtype: (str, str, int)
        """
        tile_info = get_tile_info_id(tile_id)
        return AwsService.url_to_tile(tile_info['properties']['s3Path'])

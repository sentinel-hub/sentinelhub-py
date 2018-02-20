"""
Module for creating .SAFE structure with data from AWS
"""

from .download import get_xml
from .constants import AwsConstants, EsaSafeType, MimeType
from .aws import AwsProduct, AwsTile


class SafeProduct(AwsProduct):
    """ Class inherits from `aws.AwsProduct`"""
    def get_requests(self):
        """
        Creates product structure and returns list of files for download

        :return: list of download requests
        :rtype: list(download.DownloadRequest)
        """
        safe = self.get_safe_struct()

        self.download_list = []
        self.structure_recursion(safe, self.parent_folder)
        self.sort_download_list()
        return self.download_list, self.folder_list

    def get_safe_struct(self):
        """
        Describes a structure inside tile folder of ESA product .SAFE structure

        :return: nested dictionaries representing .SAFE structure
        :rtype: dict
        """
        safe = {}
        main_folder = self.get_main_folder()
        safe[main_folder] = {}

        product_url = self.get_product_url()

        safe[main_folder][AwsConstants.AUX_DATA] = {}

        safe[main_folder][AwsConstants.DATASTRIP] = {}
        datastrip_list = self.get_datastrip_list()
        for datastrip_folder, datastrip_url in datastrip_list:
            safe[main_folder][AwsConstants.DATASTRIP][datastrip_folder] = {}
            safe[main_folder][AwsConstants.DATASTRIP][datastrip_folder][AwsConstants.QI_DATA] = {}
            safe[main_folder][AwsConstants.DATASTRIP][datastrip_folder][
                self.get_datastrip_metadata_name(datastrip_folder)] = datastrip_url + '/metadata.xml'

        safe[main_folder][AwsConstants.GRANULE] = {}

        for tile_info in self.product_info['tiles']:
            tile_name, date, aws_index = self.url_to_tile(self.get_tile_url(tile_info))
            if self.tile_list is None or AwsTile.parse_tile_name(tile_name) in self.tile_list:
                tile_struct = SafeTile(tile_name, date, aws_index, parent_folder=None, bands=self.bands,
                                       metafiles=self.metafiles).get_safe_struct()
                for tile_name, safe_struct in tile_struct.items():
                    safe[main_folder][AwsConstants.GRANULE][tile_name] = safe_struct

        safe[main_folder][AwsConstants.HTML] = {}  # AWS doesn't have this data
        safe[main_folder][AwsConstants.INFO] = {}  # AWS doesn't have this data

        safe[main_folder]['INSPIRE.xml'] = self.get_url('inspire')
        safe[main_folder]['manifest.safe'] = product_url + '/manifest.safe'
        safe[main_folder][self.get_product_metadata_name()] = self.get_url('metadata')
        if self.safe_type == EsaSafeType.OLD_SAFE_TYPE:
            safe[main_folder][edit_name(self.product_id, 'BWI') + '.png'] = product_url + '/preview.png'
        return safe

    def get_main_folder(self):
        """
        :return: name of main folder
        :rtype: str
        """
        return self.product_id + '.SAFE'

    def get_datastrip_list(self):
        """
        :return: list of datastrips folder names and urls from productInfo.json file
        :rtype: list((str, str))
        """
        datastrips = self.product_info['datastrips']
        return [(self.get_datastrip_name(datastrip['id']), self.base_url + datastrip['path'])
                for datastrip in datastrips]

    def get_datastrip_name(self, datastrip):
        """
        :param datastrip: name of datastrip
        :type datastrip: str
        :return: name of datastrip folder
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_SAFE_TYPE:
            return datastrip
        return '_'.join(datastrip.split('_')[4:9])

    def get_datastrip_metadata_name(self, datastrip_folder):
        """
        :param datastrip_folder: name of datastrip folder
        :type: str
        :return: name of datastrip metadata file
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_SAFE_TYPE:
            name = datastrip_folder.rsplit('_', 1)[0]
        if self.safe_type == EsaSafeType.COMPACT_SAFE_TYPE:
            name = 'MTD_DS'
        return '{}.{}'.format(name, MimeType.XML.value)

    def get_product_metadata_name(self):
        """
        :return: name of product metadata file
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_SAFE_TYPE:
            name = edit_name(self.product_id, 'MTD', 'SAFL1C')
        if self.safe_type == EsaSafeType.COMPACT_SAFE_TYPE:
            name = 'MTD_MSIL1C'
        return '{}.{}'.format(name, MimeType.XML.value)


class SafeTile(AwsTile):
    """ Class inherits from `aws.AwsTile`"""
    def __init__(self, *args, **kwargs):
        super(SafeTile, self).__init__(*args, **kwargs)

        self.safe_type = self.get_safe_type()
        self.tile_id = self.get_tile_id()

    def get_requests(self):
        """
        Creates tile structure and returns list of files for download.

        :return: list of download requests for
        :rtype: list(download.DownloadRequest)
        """
        safe = self.get_safe_struct()

        self.download_list = []
        self.structure_recursion(safe, self.parent_folder)
        self.sort_download_list()
        return self.download_list, self.folder_list

    def get_safe_struct(self):
        """
        Describes a structure inside tile folder of ESA product .SAFE structure.

        :return: nested dictionaries representing .SAFE structure
        :rtype: dict
        """
        safe = {}
        main_folder = self.get_main_folder()
        safe[main_folder] = {}

        safe[main_folder][AwsConstants.AUX_DATA] = {}
        safe[main_folder][AwsConstants.AUX_DATA][self.get_aux_data_name()] = self.tile_url + '/auxiliary/ECMWFT'

        safe[main_folder][AwsConstants.IMG_DATA] = {}
        for band in self.bands:
            safe[main_folder][AwsConstants.IMG_DATA][self.get_img_name(band)] = self.get_url(band)
        if self.safe_type == EsaSafeType.COMPACT_SAFE_TYPE:
            safe[main_folder][AwsConstants.IMG_DATA][self.get_img_name('TCI')] = self.get_url('TCI')

        safe[main_folder][AwsConstants.QI_DATA] = {}
        safe[main_folder][AwsConstants.QI_DATA][self.get_gml_name('CLOUDS')] = self.get_gml_url('CLOUDS')
        for qi_type in AwsConstants.QI_LIST:
            for band in self.bands:
                safe[main_folder][AwsConstants.QI_DATA][self.get_gml_name(qi_type, band)] = self.get_gml_url(qi_type,
                                                                                                             band)
        safe[main_folder][AwsConstants.QI_DATA][self.get_preview_name()] = self.tile_url + '/preview.jp2'

        safe[main_folder][self.get_tile_metadata_name()] = self.get_url(AwsConstants.METADATA)

        return safe

    def get_safe_type(self):
        """
        Determines the type of ESA product. In 2016 ESA changed structure and naming of data. Therefore the class must
        distinguish between old product type and compact (new) product type.

        :return: type of ESA product
        :rtype: constants.EsaSafeType
        """
        if self.get_product_id().split('_')[1] == 'MSIL1C':
            return EsaSafeType.COMPACT_SAFE_TYPE
        return EsaSafeType.OLD_SAFE_TYPE

    def get_tile_id(self):
        """
        Creates ESA tile ID

        :return: ESA tile ID
        :rtype: str
        """
        tree = get_xml(self.get_url(AwsConstants.METADATA))

        tile_id = tree[0].find('TILE_ID').text
        if self.safe_type == EsaSafeType.COMPACT_SAFE_TYPE:
            info = tile_id.split('_')
            tile_id = '_'.join([info[3], info[-2], info[-3], self.get_sensing_time()])
        return tile_id

    def get_sensing_time(self):
        """
        :return: Exact tile sensing time
        :rtype: str
        """
        return self.tile_info['timestamp'].split('.')[0].replace('-', '').replace(':', '')

    def get_datatake_time(self):
        """
        :return: Exact time of datatake
        :rtype: str
        """
        return self.tile_info['productName'].split('_')[2]

    def get_main_folder(self):
        """
        :return: name of tile folder
        :rtype: str
        """
        return self.tile_id

    def get_tile_metadata_name(self):
        """
        :return: name of tile metadata file
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_SAFE_TYPE:
            name = edit_name(self.tile_id, 'MTD', delete_end=True)
        if self.safe_type == EsaSafeType.COMPACT_SAFE_TYPE:
            name = 'MTD_TL'
        return name + '.xml'

    def get_aux_data_name(self):
        """
        :return: name of auxiliary data file
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_SAFE_TYPE:
            # this is not correct, but we cannot reconstruct last two timestamps in auxiliary data file name
            # e.g. S2A_OPER_AUX_ECMWFT_EPA__20160120T231011_V20160103T150000_20160104T030000
            return 'AUX_ECMWFT'
        return 'AUX_ECMWFT'

    def get_img_name(self, band):
        """
        :param band: band name
        :type band: str
        :return: name of band image file
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_SAFE_TYPE:
            name = self.tile_id.rsplit('_', 1)[0] + '_' + band
        if self.safe_type == EsaSafeType.COMPACT_SAFE_TYPE:
            name = '_'.join([self.tile_id.split('_')[1], self.get_datatake_time(), band])
        return name + '.jp2'

    def get_gml_name(self, qi_type, band='B00'):
        """
        :param qi_type: type of quality indicator
        :type qi_type: str
        :param band: band name
        :type band: str
        :return: name of gml file
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_SAFE_TYPE:
            name = edit_name(self.tile_id, 'MSK', delete_end=True)
            name = name.replace('L1C_TL', qi_type)
            name += '_' + band + '_MSIL1C'
        if self.safe_type == EsaSafeType.COMPACT_SAFE_TYPE:
            name = 'MSK_' + qi_type + '_' + band
        return name + '.gml'

    def get_gml_url(self, qi_type, band='B00'):
        """
        :param qi_type: type of quality indicator
        :type qi_type: str
        :param band: band name
        :type band: str
        :return: location of gml file on AWS
        :rtype: str
        """
        return self.tile_url + '/qi/MSK_' + qi_type + '_' + band + '.gml'

    def get_preview_name(self):
        """
        :return: name of preview file
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_SAFE_TYPE:
            name = edit_name(self.tile_id, 'PVI', delete_end=True)
        if self.safe_type == EsaSafeType.COMPACT_SAFE_TYPE:
            name = '_'.join([self.tile_id.split('_')[1], self.get_datatake_time(), 'PVI'])
        return name + '.jp2'


def edit_name(name, code, add_code=None, delete_end=False):
    """
    Helping function for creating file names in .SAFE format

    :param name: initial string
    :type name: str
    :param code:
    :type code: str
    :param add_code:
    :type add_code: str or None
    :param delete_end:
    :type delete_end: bool
    :return: edited string
    :rtype: str
    """
    info = name.split('_')
    info[2] = code
    if add_code is not None:
        info[3] = add_code
    if delete_end:
        info.pop()
    return '_'.join(info)

"""
Module for creating .SAFE structure with data from AWS
"""

import warnings

from .aws import AwsProduct, AwsTile
from .constants import AwsConstants, EsaSafeType, MimeType, DataSource
from .download.aws_client import get_aws_xml
from .exceptions import SHRuntimeWarning


class SafeProduct(AwsProduct):
    """ Class implementing transformation of satellite products from AWS into .SAFE structure
    """

    def get_requests(self):
        """ Creates product structure and returns list of files for download

        :return: list of download requests
        :rtype: list(download.DownloadRequest)
        """
        safe = self.get_safe_struct()

        self.download_list = []
        self.structure_recursion(safe, self.parent_folder)
        self.sort_download_list()
        return self.download_list, self.folder_list

    def get_safe_struct(self):
        """ Describes a structure inside tile folder of ESA product .SAFE structure

        :return: nested dictionaries representing .SAFE structure
        :rtype: dict
        """
        safe = {}
        main_folder = self.get_main_folder()
        safe[main_folder] = {}

        safe[main_folder][AwsConstants.AUX_DATA] = {}

        safe[main_folder][AwsConstants.DATASTRIP] = {}
        datastrip_list = self.get_datastrip_list()
        for datastrip_folder, datastrip_url in datastrip_list:
            safe[main_folder][AwsConstants.DATASTRIP][datastrip_folder] = {}
            safe[main_folder][AwsConstants.DATASTRIP][datastrip_folder][AwsConstants.QI_DATA] = {}
            # S-2 L1C reports are on AWS only stored with tiles and without RADIOMETRIC_QUALITY
            if self.has_reports() and self.data_source is DataSource.SENTINEL2_L2A:
                for metafile in AwsConstants.QUALITY_REPORTS:
                    metafile_name = self.add_file_extension(metafile)
                    safe[main_folder][AwsConstants.DATASTRIP][datastrip_folder][AwsConstants.QI_DATA][
                        metafile_name] = '{}/qi/{}'.format(datastrip_url, metafile_name)

            safe[main_folder][AwsConstants.DATASTRIP][datastrip_folder][
                self.get_datastrip_metadata_name(datastrip_folder)] = '{}/{}'.format(
                    datastrip_url, self.add_file_extension(AwsConstants.METADATA))

        safe[main_folder][AwsConstants.GRANULE] = {}

        for tile_info in self.product_info['tiles']:
            tile_name, date, aws_index = self.url_to_tile(self.get_tile_url(tile_info))
            if self.tile_list is None or AwsTile.parse_tile_name(tile_name) in self.tile_list:
                tile_struct = SafeTile(tile_name, date, aws_index, parent_folder=None, bands=self.bands,
                                       metafiles=self.metafiles, data_source=self.data_source).get_safe_struct()
                for tile_name, safe_struct in tile_struct.items():
                    safe[main_folder][AwsConstants.GRANULE][tile_name] = safe_struct

        safe[main_folder][AwsConstants.HTML] = {}  # AWS doesn't have this data
        safe[main_folder][AwsConstants.INFO] = {}  # AWS doesn't have this data

        safe[main_folder][self.get_product_metadata_name()] = self.get_url(AwsConstants.METADATA)
        safe[main_folder]['INSPIRE.xml'] = self.get_url(AwsConstants.INSPIRE)
        safe[main_folder][self.add_file_extension(AwsConstants.MANIFEST)] = self.get_url(AwsConstants.MANIFEST)

        if self.is_early_compact_l2a():
            safe[main_folder]['L2A_Manifest.xml'] = self.get_url(AwsConstants.L2A_MANIFEST)
            safe[main_folder][self.get_report_name()] = self.get_url(AwsConstants.REPORT)

        if self.safe_type == EsaSafeType.OLD_TYPE and self.baseline != '02.02':
            safe[main_folder][_edit_name(self.product_id, 'BWI') + '.png'] = self.get_url(AwsConstants.PREVIEW,
                                                                                          MimeType.PNG)
        return safe

    def get_main_folder(self):
        """
        :return: name of main folder
        :rtype: str
        """
        return '{}.SAFE'.format(self.product_id)

    def get_datastrip_list(self):
        """
        :return: list of datastrips folder names and urls from `productInfo.json` file
        :rtype: list((str, str))
        """
        datastrips = self.product_info['datastrips']
        return [(self.get_datastrip_name(datastrip['id']), '{}/{}'.format(self.base_url, datastrip['path']))
                for datastrip in datastrips]

    def get_datastrip_name(self, datastrip):
        """
        :param datastrip: name of datastrip
        :type datastrip: str
        :return: name of datastrip folder
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_TYPE:
            return datastrip
        return '_'.join(datastrip.split('_')[4:-1])

    def get_datastrip_metadata_name(self, datastrip_folder):
        """
        :param datastrip_folder: name of datastrip folder
        :type datastrip_folder: str
        :return: name of datastrip metadata file
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_TYPE:
            name = _edit_name(datastrip_folder, 'MTD', delete_end=True)
        else:
            name = 'MTD_DS'
        return '{}.{}'.format(name, MimeType.XML.value)

    def get_product_metadata_name(self):
        """
        :return: name of product metadata file
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_TYPE:
            name = _edit_name(self.product_id, 'MTD', 'SAFL1C')
        else:
            name = 'MTD_{}'.format(self.product_id.split('_')[1])
        return '{}.{}'.format(name, MimeType.XML.value)

    def get_report_name(self):
        """
        :return: name of the report file of L2A products
        :rtype: str
        """
        return '{}_{}_report.{}'.format(self.product_id, self.get_report_time(), MimeType.XML.value)

    def get_report_time(self):
        """ Returns time when the L2A processing started and reports was created.
        :return: String in a form YYYYMMDDTHHMMSS
        :rtype: str
        """
        tree = get_aws_xml(self.get_url(AwsConstants.REPORT))

        try:
            timestamp = tree.find('check/inspection').attrib['execution']
            return timestamp.split(',')[0].replace(' ', 'T').replace(':', '').replace('-', '')
        except AttributeError:
            warnings.warn('Could not obtain the L2A report creation time', category=SHRuntimeWarning)
            return 'unknown'


class SafeTile(AwsTile):
    """ Class implementing transformation of satellite tiles from AWS into .SAFE structure
    """
    def __init__(self, *args, **kwargs):
        """ Initialization parameters are inherited from parent class
        """
        super().__init__(*args, **kwargs)

        self.tile_id = self.get_tile_id()

    def get_requests(self):
        """ Creates tile structure and returns list of files for download.

        :return: list of download requests for
        :rtype: list(download.DownloadRequest)
        """
        safe = self.get_safe_struct()

        self.download_list = []
        self.structure_recursion(safe, self.parent_folder)
        self.sort_download_list()
        return self.download_list, self.folder_list

    def get_safe_struct(self):
        """ Describes a structure inside tile folder of ESA product .SAFE structure.

        :return: nested dictionaries representing .SAFE structure
        :rtype: dict
        """
        # pylint: disable=too-many-branches
        safe = {}
        main_folder = self.get_main_folder()
        safe[main_folder] = {}

        safe[main_folder][AwsConstants.AUX_DATA] = {}
        # Not sure if 2nd condition of the following is correct:
        if self.data_source is not DataSource.SENTINEL2_L1C or self.baseline != '02.04':
            ecmwft_file = AwsConstants.ECMWFT if self.data_source is DataSource.SENTINEL2_L1C or \
                                                 self.safe_type is EsaSafeType.OLD_TYPE else AwsConstants.AUX_ECMWFT
            safe[main_folder][AwsConstants.AUX_DATA][self.get_aux_data_name()] = self.get_url(ecmwft_file)
        # Old products also have DEM and MSI in aux folder

        if self.is_early_compact_l2a():
            safe[main_folder][AwsConstants.AUX_DATA][self.add_file_extension(AwsConstants.GIPP, remove_path=True)] =\
                self.get_url(AwsConstants.GIPP)

        safe[main_folder][AwsConstants.IMG_DATA] = {}
        if self.data_source is DataSource.SENTINEL2_L1C:
            for band in self.bands:
                safe[main_folder][AwsConstants.IMG_DATA][self.get_img_name(band)] = self.get_url(band)
            if self.safe_type == EsaSafeType.COMPACT_TYPE:
                safe[main_folder][AwsConstants.IMG_DATA][self.get_img_name(AwsConstants.TCI)] =\
                    self.get_url(AwsConstants.TCI)
        else:
            for resolution in AwsConstants.RESOLUTIONS:
                safe[main_folder][AwsConstants.IMG_DATA][resolution] = {}
            for band_name in self.bands:
                resolution, band = band_name.split('/')
                if self._band_exists(band_name):
                    safe[main_folder][AwsConstants.IMG_DATA][resolution][self.get_img_name(band, resolution)] =\
                        self.get_url(band_name)

        safe[main_folder][AwsConstants.QI_DATA] = {}
        safe[main_folder][AwsConstants.QI_DATA][self.get_qi_name('CLOUDS')] = self.get_gml_url('CLOUDS')
        for qi_type in AwsConstants.QI_LIST:
            for band in AwsConstants.S2_L1C_BANDS:
                safe[main_folder][AwsConstants.QI_DATA][self.get_qi_name(qi_type, band)] = self.get_gml_url(qi_type,
                                                                                                            band)

        if self.has_reports():
            for metafile in AwsConstants.QUALITY_REPORTS:
                if metafile == AwsConstants.RADIOMETRIC_QUALITY and self.data_source is DataSource.SENTINEL2_L2A and \
                   self.baseline <= '02.07':
                    continue

                metafile_name = self.add_file_extension(metafile, remove_path=True)
                safe[main_folder][AwsConstants.QI_DATA][metafile_name] = self.get_qi_url(metafile_name)

        if self.data_source is DataSource.SENTINEL2_L2A:
            for mask in AwsConstants.CLASS_MASKS:
                for resolution in [AwsConstants.R20m, AwsConstants.R60m]:
                    if self.baseline <= '02.06':
                        mask_name = self.get_img_name(mask, resolution)
                    else:
                        mask_name = self.get_qi_name('{}PRB'.format(mask), resolution.lstrip('R'), MimeType.JP2)
                    safe[main_folder][AwsConstants.QI_DATA][mask_name] =\
                        self.get_qi_url('{}_{}.jp2'.format(mask, resolution.lstrip('R')))

        if self.is_early_compact_l2a():
            safe[main_folder][AwsConstants.QI_DATA][self.get_img_name(AwsConstants.PVI)] = self.get_preview_url('L2A')

        preview_type = 'L2A' if self.data_source is DataSource.SENTINEL2_L2A and self.baseline >= '02.07' else 'L1C'
        safe[main_folder][AwsConstants.QI_DATA][self.get_preview_name()] = self.get_preview_url(preview_type)

        safe[main_folder][self.get_tile_metadata_name()] = self.get_url(AwsConstants.METADATA)

        return safe

    def get_tile_id(self):
        """ Creates ESA tile ID

        :return: ESA tile ID
        :rtype: str
        """
        tree = get_aws_xml(self.get_url(AwsConstants.METADATA))

        tile_id_tag = 'TILE_ID_2A' if self.data_source is DataSource.SENTINEL2_L2A and self.baseline <= '02.06' else\
            'TILE_ID'
        tile_id = tree[0].find(tile_id_tag).text
        if self.safe_type is EsaSafeType.OLD_TYPE:
            return tile_id

        info = tile_id.split('_')

        if (self.data_source is DataSource.SENTINEL2_L1C and self.baseline >= '02.07') or \
           (self.data_source is DataSource.SENTINEL2_L2A and self.baseline >= '02.10'):
            tile_id_time = self.get_datastrip_time()
        else:
            tile_id_time = self.get_sensing_time()

        return '_'.join([info[3], info[-2], info[-3], tile_id_time])

    def get_sensing_time(self):
        """
        :return: Exact tile sensing time
        :rtype: str
        """
        return self.tile_info['timestamp'].split('.')[0].replace('-', '').replace(':', '')

    def get_datastrip_time(self):
        """
        :return: Exact datastrip time
        :rtype: str
        """
        # S2A_OPER_MSI_L1C_DS_EPAE_20181119T061056_S20181119T031012_N02.07 -> 20181119T031012
        # S2A_OPER_MSI_L1C_DS_EPA__20190225T132350_S20190129T144524_N02.07 -> 20190129T144524
        return self.tile_info['datastrip']['id'].replace('__', '_').split('_')[7].lstrip('S')

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
        if self.safe_type == EsaSafeType.OLD_TYPE:
            name = _edit_name(self.tile_id, 'MTD', delete_end=True)
        else:
            name = 'MTD_TL'
        return '{}.xml'.format(name)

    def get_aux_data_name(self):
        """
        :return: name of auxiliary data file
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_TYPE:
            # this is not correct, but we cannot reconstruct last two timestamps in auxiliary data file name
            # e.g. S2A_OPER_AUX_ECMWFT_EPA__20160120T231011_V20160103T150000_20160104T030000
            return 'AUX_ECMWFT'
        return 'AUX_ECMWFT'

    def get_img_name(self, band, resolution=None):
        """
        :param band: band name
        :type band: str
        :param resolution: Specifies the resolution in case of Sentinel-2 L2A products
        :type resolution: str or None
        :return: name of band image file
        :rtype: str
        """
        band = band.split('/')[-1]
        if self.safe_type is EsaSafeType.OLD_TYPE:
            name = self.tile_id.rsplit('_', 1)[0] + '_' + band
        else:
            name = '_'.join([self.tile_id.split('_')[1], self.get_datatake_time(), band])
        if self.data_source is DataSource.SENTINEL2_L2A and resolution is not None:
            name = '{}_{}'.format(name, resolution.lstrip('R'))
        if self.data_source is DataSource.SENTINEL2_L2A and self.baseline <= '02.06':
            name = 'L2A_{}'.format(name)
        return '{}.jp2'.format(name)

    def get_qi_name(self, qi_type, band='B00', data_format=MimeType.GML):
        """
        :param qi_type: type of quality indicator
        :type qi_type: str
        :param band: band name
        :type band: str
        :param data_format: format of the file
        :type data_format: MimeType
        :return: name of gml file
        :rtype: str
        """
        band = band.split('/')[-1]
        if self.safe_type == EsaSafeType.OLD_TYPE:
            name = _edit_name(self.tile_id, 'MSK', delete_end=True)
            source_param = '{}_TL'.format('L1C' if self.data_source is DataSource.SENTINEL2_L1C else 'L2A')
            name = name.replace(source_param, qi_type)
            name = '{}_{}_MSIL1C'.format(name, band)
        else:
            name = 'MSK_{}_{}'.format(qi_type, band)
        return '{}.{}'.format(name, data_format.value)

    def get_preview_name(self):
        """ Returns .SAFE name of full resolution L1C preview
        :return: name of preview file
        :rtype: str
        """
        if self.safe_type == EsaSafeType.OLD_TYPE:
            name = _edit_name(self.tile_id, AwsConstants.PVI, delete_end=True)
        else:
            name = '_'.join([self.tile_id.split('_')[1], self.get_datatake_time(), AwsConstants.PVI])
        return '{}.jp2'.format(name)


def _edit_name(name, code, add_code=None, delete_end=False):
    """ Helping function for creating file names in .SAFE format

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

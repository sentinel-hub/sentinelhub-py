"""
Script for creating safe structure of data from
http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com


Product url examples:
http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/#products/2016/1/3/S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_20160103T171947/
http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/#products/2017/4/14/S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551/
Tile url examples:
http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/#tiles/13/P/HS/2016/1/3/0/
http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/#tiles/54/H/VH/2017/4/14/0/
"""

from xml.etree import ElementTree

from . import download


DEFAULT_DATA_LOCATION = '.'

TILE_INFO = 'tileInfo.json'
PRODUCT_INFO = 'productInfo.json'

BANDS = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12']
QI_LIST = ['DEFECT', 'DETFOO', 'NODATA', 'SATURA', 'TECQUA']

MAIN_URL = 'http://sentinel-s2-l1c.s3.amazonaws.com'

REDOWNLOAD = False
THREADED_DOWNLOAD = False

AUX_DATA = 'AUX_DATA'
DATASTRIP = 'DATASTRIP'
GRANULE = 'GRANULE'
HTML = 'HTML'
INFO = 'rep_info'
QI_DATA = 'QI_DATA'
IMG_DATA = 'IMG_DATA'

DATE_SEPARATOR = '-'

OLD_SAFE_TYPE = 'old type'
COMPACT_SAFE_TYPE = 'compact type'
TYPE_CHANGE_DATE = DATE_SEPARATOR.join(['2016', '12', '06'])

# Examples
# old format: S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_20160103T171947
# compact format: S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551
class SafeProduct():
    def __init__(self, product_id, folder=DEFAULT_DATA_LOCATION, bands=BANDS):
        self.folder = folder
        self.product_id = product_id
        self.bands = bands

        validate_bands(bands)

        self.read_structure()

    def read_structure(self):
        self.safe_type = self.get_safe_type()
        self.date = self.get_date()

        self.product_info = download.get_json(self.get_product_url() + '/' + PRODUCT_INFO)

        self.tile_list = [SafeTile(url=self.get_tile_url(self.product_info['tiles'][i]), bands=self.bands) for i in range(len(self.product_info['tiles']))]

        self.safe = None

    def get_structure(self):
        safe = {}
        main_folder = self.get_main_folder()
        safe[main_folder] = {}

        product_url = self.get_product_url()

        safe[main_folder][AUX_DATA] = {}

        safe[main_folder][DATASTRIP] = {}
        datastrip_list = self.get_datastrip_list()
        for datastrip_folder, datastrip_url in datastrip_list:
            safe[main_folder][DATASTRIP][datastrip_folder] = {}
            safe[main_folder][DATASTRIP][datastrip_folder][QI_DATA] = {}
            safe[main_folder][DATASTRIP][datastrip_folder][self.get_datastrip_metadata_name(datastrip_folder)] = datastrip_url + '/metadata.xml'

        safe[main_folder][GRANULE] = {}
        for safe_tile in self.tile_list:
            tile_struct = safe_tile.get_structure()
            for tile_name, safe_struct in tile_struct.items():
                safe[main_folder][GRANULE][tile_name] = safe_struct

        safe[main_folder][HTML] = {}
        # aws doesn't have this data

        safe[main_folder][INFO] = {}
        # aws doesn't have this data

        safe[main_folder]['INSPIRE.xml'] = product_url + '/inspire.xml'
        safe[main_folder]['manifest.safe'] = product_url + '/manifest.safe'
        safe[main_folder][self.get_product_metadata_name()] = product_url + '/metadata.xml'
        if self.safe_type == OLD_SAFE_TYPE:
            safe[main_folder][edit_name(self.product_id, 'BWI') + '.png'] = product_url + '/preview.png'

        return safe

    def download_structure(self, redownload=REDOWNLOAD, threaded_download=THREADED_DOWNLOAD):
        download_list = self.get_download_list(True)
        download.download_data(download_list, redownload, threaded_download)

    def get_download_list(self, create_folders=False):
        if self.safe is None:
            self.safe = self.get_structure()
        download_list = []
        structure_recursion(self.safe, self.folder, download_list, create_folders)
        return download_list

    def set_folder(self, new_folder):
        self.folder = new_folder

    def set_product_id(self, newproduct_id):
        self.product_id = newproduct_id
        self.read_structure()

    def get_main_folder(self):
        return self.product_id + '.SAFE'

    def get_safe_type(self):
        if self.product_id.split('_')[1] == 'MSIL1C':
            return COMPACT_SAFE_TYPE
        return OLD_SAFE_TYPE

    def get_date(self):
        if self.safe_type == OLD_SAFE_TYPE:
            name = self.product_id.split('_')[-2]
            date = [name[1:5], name[5:7], name[7:9]]
        if self.safe_type == COMPACT_SAFE_TYPE:
            name = self.product_id.split('_')[2]
            date = [name[:4], name[4:6], name[6:8]]
        return DATE_SEPARATOR.join(date_part.lstrip('0') for date_part in date)

    # Example: http://sentinel-s2-l1c.s3.amazonaws.com/products/2017/4/14/S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551
    def get_product_url(self):
        return MAIN_URL + '/products/' + self.date.replace(DATE_SEPARATOR, '/') + '/' + self.product_id

    def get_datastrip_list(self):
        datastrips = self.product_info['datastrips']
        return [(self.get_datastrip_name(datastrips[i]['id']), MAIN_URL + '/' + datastrips[i]['path']) for i in range(len(datastrips))]

    # old format: S2A_OPER_MSI_L1C_DS_EPA__20160120T231011_S20160103T171621_N02.01
    # compact format: DS_SGS__20170414T033348_S20170414T003551
    def get_datastrip_name(self, datastrip):
        if self.safe_type == OLD_SAFE_TYPE:
            return datastrip
        if self.safe_type == COMPACT_SAFE_TYPE:
            return '_'.join(datastrip.split('_')[4:9])

    def get_datastrip_metadata_name(self, datastrip_folder):
        if self.safe_type == OLD_SAFE_TYPE:
            name = '_'.join(datastrip_folder.split('_')[:-1])
        if self.safe_type == COMPACT_SAFE_TYPE:
            name = 'MTD_DS'
        return name + '.xml'

    def get_product_metadata_name(self):
        if self.safe_type == OLD_SAFE_TYPE:
            name = edit_name(self.product_id, 'MTD', 'SAFL1C')
        if self.safe_type == COMPACT_SAFE_TYPE:
            name = 'MTD_MSIL1C'
        return name + '.xml'

    def get_tile_url(self, tile_info):
        return MAIN_URL + '/' + tile_info['path']


class SafeTile():
    def __init__(self, tile_id=None, url=None, tile_name=None, date=None, folder='.', bands=BANDS):
        self.folder = folder
        self.tile_id = tile_id
        self.tile_url = url
        self.tile_name = tile_name
        self.date = date
        self.bands = bands

        validate_bands(bands)

        self.read_structure()

    def read_structure(self):
        if self.tile_url is not None:
            self.tile_url = self.tile_url.rstrip('/')
        if self.tile_name is not None:
            self.tile_name = self.tile_name.lstrip('T0')
        if self.date is not None:
            self.date = DATE_SEPARATOR.join(date_part.lstrip('0') for date_part in self.date.split(DATE_SEPARATOR))

        if self.tile_url is not None or (self.tile_name is not None and self.date is not None):
            if self.tile_url is not None:
                self.tile_name, self.date = url_to_namedate(self.tile_url)
            else:
                self.tile_url = namedate_to_url(self.tile_name, self.date)
            self.tile_info = self.get_tile_info()
            self.safe_type = self.get_safe_type()
            self.tile_id = self.url_to_tile_id()
        elif self.tile_id is not None:
            self.tile_name, self.date = tile_id_to_namedate(self.tile_id)
            self.tile_url = namedate_to_url(self.tile_name, self.date)
            self.tile_info = self.get_tile_info()
            self.safe_type = self.get_safe_type()

        self.safe = None

    def get_structure(self):
        safe = {}
        main_folder = self.get_main_folder()
        safe[main_folder] = {}

        safe[main_folder][AUX_DATA] = {}
        safe[main_folder][AUX_DATA][self.get_aux_data_name()] = self.tile_url + '/auxiliary/ECMWFT'

        safe[main_folder][IMG_DATA] = {}
        for band in self.bands:
            safe[main_folder][IMG_DATA][self.get_img_name(band)] = self.tile_url + '/' + band + '.jp2'
        if self.safe_type == COMPACT_SAFE_TYPE:
            safe[main_folder][IMG_DATA][self.get_img_name('TCI')] = self.tile_url + '/TCI.jp2'

        safe[main_folder][QI_DATA] = {}
        safe[main_folder][QI_DATA][self.get_gml_name('CLOUDS')] = self.get_gml_url('CLOUDS')
        for qi_type in QI_LIST:
            for band in self.bands:
                safe[main_folder][QI_DATA][self.get_gml_name(qi_type, band)] = self.get_gml_url(qi_type, band)
        safe[main_folder][QI_DATA][self.get_preview_name()] = self.tile_url + '/preview.jp2'

        safe[main_folder][self.get_tile_metadata_name()] = self.tile_url + '/metadata.xml'

        return safe

    def download_structure(self, redownload=REDOWNLOAD, threaded_download=THREADED_DOWNLOAD):
        download_list = self.get_download_list(True)
        download.download_data(download_list, redownload, threaded_download)

    def get_download_list(self, create_folders=False):
        if self.safe is None:
            self.safe = self.get_structure()
        download_list = []
        structure_recursion(self.safe, self.folder, download_list, create_folders)
        return download_list

    def set_folder(self, new_folder):
        self.folder = new_folder

    def url_to_tile_id(self):
        response = download.make_request(self.tile_url + '/metadata.xml', return_data=True, verbose=False)
        tree = ElementTree.fromstring(response.content)
        tile_id = tree[0].find('TILE_ID').text
        if self.safe_type == COMPACT_SAFE_TYPE:
            info = tile_id.split('_')
            tile_id = '_'.join([info[3], info[-2], info[-3], self.get_sensing_time()])
        return tile_id

    def get_tile_info(self):
        try:
            return download.get_json(self.tile_url + '/' + TILE_INFO)
        except:
            self.increase_url()
            return download.get_json(self.tile_url + '/' + TILE_INFO)

    # .../tiles/38/T/ML/2015/12/19/0 -> .../tiles/38/T/ML/2015/12/19/1
    def increase_url(self):
        info = self.tile_url.split('/')
        info[-1] = str(int(info[-1]) + 1)
        self.tile_url = '/'.join(info)

    def get_product_id(self):
        return self.tile_info['productName']

    def get_sensing_time(self):
        return self.tile_info['timestamp'].split('.')[0].replace('-', '').replace(':', '')

    def get_datatake_time(self):
        return self.tile_info['productName'].split('_')[2]

    def get_main_folder(self):
        return self.tile_id

    def get_safe_type(self):
        if self.get_product_id().split('_')[1] == 'MSIL1C':
            return COMPACT_SAFE_TYPE
        return OLD_SAFE_TYPE

    def get_tile_metadata_name(self):
        if self.safe_type == OLD_SAFE_TYPE:
            name = edit_name(self.tile_id, 'MTD', delete_end=True)
        if self.safe_type == COMPACT_SAFE_TYPE:
            name = 'MTD_TL'
        return name + '.xml'

    def get_aux_data_name(self):
        if self.safe_type == OLD_SAFE_TYPE:
            return 'AUX_ECMWFT' # this is not correct, but we cannot reconstruct last two timestamps in name S2A_OPER_AUX_ECMWFT_EPA__20160120T231011_V20160103T150000_20160104T030000
        if self.safe_type == COMPACT_SAFE_TYPE:
            return 'AUX_ECMWFT'

    # old format: S2A_OPER_MSI_L1C_TL_EPA__20160120T231011_A002783_T13PHS_B01
    # compact format: T54HVH_20170414T003551_B01
    def get_img_name(self, band):
        if self.safe_type == OLD_SAFE_TYPE:
            info = self.tile_id.split('_')
            info[-1] = band
            name = '_'.join(info)
        if self.safe_type == COMPACT_SAFE_TYPE:
            name = '_'.join([self.tile_id.split('_')[1], self.get_datatake_time(), band])
        return name + '.jp2'

    # old format: S2A_OPER_MSK_DEFECT_EPA__20160120T231011_A002783_T13PHS_B01_MSIL1C
    # compact format: MSK_CLOUDS_B00
    def get_gml_name(self, qi_type, band='B00'):
        if self.safe_type == OLD_SAFE_TYPE:
            name = edit_name(self.tile_id, 'MSK', delete_end=True)
            name = name.replace('L1C_TL', qi_type)
            name += '_' + band + '_MSIL1C'
        if self.safe_type == COMPACT_SAFE_TYPE:
            name = 'MSK_' + qi_type + '_' + band
        return name + '.gml'

    def get_gml_url(self, qi_type, band='B00'):
        return self.tile_url + '/qi/MSK_' + qi_type + '_' + band + '.gml'

    def get_preview_name(self):
        if self.safe_type == OLD_SAFE_TYPE:
            name = edit_name(self.tile_id, 'PVI', delete_end=True)
        if self.safe_type == COMPACT_SAFE_TYPE:
            name = '_'.join([self.tile_id.split('_')[1], self.get_datatake_time(), 'PVI'])
        return name + '.jp2'

# old format: S2A_OPER_MSI_L1C_TL_EPA__20160120T231011_A002783_T13PHS_N02.01
# compact format: L1C_T54HVH_A009451_20170414T003551
def tile_id_to_namedate(tile_id):
    info = tile_id.split('_')
    if tile_id[:2] == 'S2A': # for old format this isn't possible
        raise Exception('Cannot find tile from tile ID in old format')
    if tile_id[:2] == 'L1C': # compact format
        name = info[1].lstrip('T')
        time = info[-1]
        date = [time[:4], time[4:6], time[6:8]]
        date = DATE_SEPARATOR.join(date_part.lstrip('0') for date_part in date)
    return name, date

def namedate_to_url(name, date, index=0):
    return '/'.join([MAIN_URL, 'tiles', name[:-3], name[-3], name[-2:], date.replace(DATE_SEPARATOR, '/'), str(index)])

# "tiles/13/P/HS/2016/1/3/0"
def url_to_namedate(url):
    info = url.split('/')
    name = ''.join(info[-7: -4])
    date = DATE_SEPARATOR.join(info[-4: -1])
    return name, date

def edit_name(name, code, add_code=None, delete_end=False):
    info = name.split('_')
    info[2] = code
    if add_code is not None:
        info[3] = add_code
    if delete_end:
        info.pop()
    return '_'.join(info)

def structure_recursion(struct, folder, download_list, create_folders):
    if create_folders and len(struct) == 0:
        download.make_folder(folder)
    for name, substruct in struct.items():
        subfolder = folder + '/' + name
        if not isinstance(substruct, dict):
            download_list.append((substruct, subfolder))
        else:
            structure_recursion(substruct, subfolder, download_list, create_folders)

def validate_bands(bands):
    invalid = set(bands) - set(BANDS).intersection(bands)
    if bool(invalid):
        raise Exception('Invalid bands specified: ' + str(list(invalid)))

### Public functions:

def get_safe_format(product_id=None, tile=None, entire_product=False):
    if tile is not None:
        safe_tile = SafeTile(tile_name=tile[0], date=tile[1])
        if not entire_product:
            return safe_tile.get_structure()
        product_id = safe_tile.get_product_id()
    if product_id is not None:
        safe_product = SafeProduct(product_id)
        return safe_product.get_structure()

def download_safe_format(product_id=None, tile=None, folder=DEFAULT_DATA_LOCATION, redownload=REDOWNLOAD, threaded_download=THREADED_DOWNLOAD, entire_product=False, bands=BANDS):
    bands = BANDS if bands is None else bands
    if tile is not None:
        safe_tile = SafeTile(tile_name=tile[0], date=tile[1], folder=folder, bands=bands)
        if not entire_product:
            return safe_tile.download_structure(redownload=redownload, threaded_download=threaded_download)
        product_id = safe_tile.get_product_id()
    if product_id is not None:
        safe_product = SafeProduct(product_id, folder=folder, bands=bands)
        return safe_product.download_structure(redownload=redownload, threaded_download=threaded_download)

if __name__ == '__main__':
    pass
    # Examples:
    #download_safe_format('S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_20160103T171947')
    #download_safe_format('S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551')
    #download_safe_format(tile=('T38TML','2015-12-19'), entire_product=True)
    #download_safe_format(tile=('T54HVH','2017-04-14'))

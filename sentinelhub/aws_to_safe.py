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

class InvalidInputNameError(Exception):
    pass

# Examples
# old format: S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_20160103T171947
# compact format: S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551
class SafeProduct():
    def __init__(self, productId, folder=DEFAULT_DATA_LOCATION):
        self.folder = folder
        self.productId = productId

        self.read_structure()

    def read_structure(self):
        self.check_input_name()

        self.safeType = self.get_safe_type()
        self.date = self.get_date()

        self.productInfo = download.get_json(self.get_product_url() + '/productInfo.json')

        self.tileList = [SafeTile(url=self.get_tile_url(self.productInfo['tiles'][i])) for i in range (len(self.productInfo['tiles']))]

        self.safe = None


    def get_structure(self):
        self.safe = {}
        mainFolder = self.get_main_folder()
        self.safe[mainFolder] = {}

        productUrl = self.get_product_url()

        self.safe[mainFolder][AUX_DATA] = {}

        self.safe[mainFolder][DATASTRIP] = {}
        datastripList = self.get_datastrip_list()
        for datastripFolder, datastripUrl in datastripList:
            self.safe[mainFolder][DATASTRIP][datastripFolder] = {}
            self.safe[mainFolder][DATASTRIP][datastripFolder][QI_DATA] = {}
            self.safe[mainFolder][DATASTRIP][datastripFolder][self.get_datastrip_metadata_name(datastripFolder)] = datastripUrl + '/metadata.xml'

        self.safe[mainFolder][GRANULE] = {}
        for safeTile in self.tileList:
            tile_struct = safeTile.get_structure()
            for tileName in tile_struct.keys():
                self.safe[mainFolder][GRANULE][tileName] = tile_struct[tileName]

        self.safe[mainFolder][HTML] = {}
        # aws doesn't have this data

        self.safe[mainFolder][INFO] = {}
        # aws doesn't have this data

        self.safe[mainFolder]['INSPIRE.xml'] = productUrl + '/inspire.xml'
        self.safe[mainFolder]['manifest.safe'] = productUrl + '/manifest.safe'
        self.safe[mainFolder][self.get_product_metadata_name()] = productUrl + '/metadata.xml'
        if self.safeType == OLD_SAFE_TYPE:
            self.safe[mainFolder][edit_name(self.productId, 'BWI') + '.png'] = productUrl + '/preview.png'

        return self.safe

    def download_structure(self, redownload=REDOWNLOAD, threadedDownload=THREADED_DOWNLOAD):
        self.get_download_list(True)
        #print(self.downloadList)
        download.download_data(self.downloadList, redownload, threadedDownload)

    def get_download_list(self, createFolders=False):
        if self.safe is None:
            self.get_structure()
        self.downloadList = []
        structure_recursion(self.safe, self.folder, self.downloadList, createFolders)
        return self.downloadList

    def set_folder(self, newFolderLocation):
        self.folderLocation = newFolderLocation

    def set_productId(self, newProductId):
        self.productId = newProductId
        self.read_structure()

    def get_main_folder(self):
        return self.productId + '.SAFE'

    def get_safe_type(self):
        if self.productId.split('_')[1] == 'MSIL1C':
            return COMPACT_SAFE_TYPE
        return OLD_SAFE_TYPE

    def get_date(self):
        if self.safeType == OLD_SAFE_TYPE:
            name = self.productId.split('_')[-2] #### not sure if this is correct!
            date = [name[1:5], name[5:7].lstrip('0'), name[7:9]]
        if self.safeType == COMPACT_SAFE_TYPE:
            name = self.productId.split('_')[2]
            date = [name[:4], name[4:6], name[6:8]]
        return DATE_SEPARATOR.join(list(map(lambda x: x.lstrip('0'), date)))

    # http://sentinel-s2-l1c.s3.amazonaws.com/products/2017/4/14/S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551/productInfo.json
    def get_product_url(self):
        return MAIN_URL + '/products/' + self.date.replace(DATE_SEPARATOR, '/') + '/' + self.productId

    def get_datastrip_list(self):
        # I think self.productInfo['datastrips'] might be list or dict
        datastrips = self.productInfo['datastrips']
        return [(self.get_datastrip_name(datastrips[i]['id']), MAIN_URL + '/' + datastrips[i]['path']) for i in range (len(datastrips))]

    # S2A_OPER_MSI_L1C_DS_SGS__20170414T033348_S20170414T003551_N02.04
    def get_datastrip_name(self, datastrip):
        if self.safeType == OLD_SAFE_TYPE:
            return datastrip
        if self.safeType == COMPACT_SAFE_TYPE:
            return '_'.join(datastrip.split('_')[4:9])

    def get_datastrip_metadata_name(self, datastripFolder):
        if self.safeType == OLD_SAFE_TYPE:
            name = '_'.join(datastripFolder.split('_')[:-1])
        if self.safeType == COMPACT_SAFE_TYPE:
            name = 'MTD_DS'
        return name + '.xml'

    def get_product_metadata_name(self):
        if self.safeType == OLD_SAFE_TYPE:
            name = edit_name(self.productId, 'MTD', 'SAFL1C')
        if self.safeType == COMPACT_SAFE_TYPE:
            name = 'MTD_MSIL1C'
        return name + '.xml'

    def get_tile_url(self, tileInfo):
        return MAIN_URL + '/' + tileInfo['path']

    # old format: S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_20160103T171947
    # compact format: S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551
    def check_input_name(self):
        pass
        '''info = self.productId.split('_')
        if info[0] not in {'S2A', 'S2B'}': # mission id
            raise InvalidInputNameError(self.make_error_message([], '<mission id>'))
        if len(info) < 2 or info[2] not in {'OPER', 'TEST', 'MSIL1C'}:
            raise InvalidInputNameError(self.make_error_message(info[:1], '<file class/file name>'))
        if info[1] != 'MSIL1C':
            if len(info) < 3 or info[3]!='PRD':
                raise InvalidInputNameError(self.make_error_message(info[:2], 'PRD'))
            if len(info) < 4 or info[3] not in {''}:
                raise InvalidInputNameError(self.make_error_message(info[:2], 'PRD'))
        else:

    def make_error_message(self, info, nextPart=None, finalPart=False):
        if nextPart is None:
            'Too long product Id'
        msg = '_'.join(info + [nextPart])
        if not finalPart:
            return 'Product Id must start with ' + msg + '_*'
        return 'Product Id must be ' + msg'''


class SafeTile():
    def __init__(self, tileId=None, url=None, tileName=None, date=None, folder='.'):
        self.folder = folder
        self.tileId = tileId
        self.tileUrl = url
        self.tileName = tileName
        self.date = date

        self.read_structure()

    def read_structure(self):
        self.tileInfo = download.get_json(self.tileUrl + '/productInfo.json')
        self.safeType = self.get_safe_type()

        if self.tileId is not None:
            self.tile_id_to_name()
            self.name_to_url() # url index ?
        elif self.tileUrl is not None:
            self.url_to_name()
            self.url_to_tile_id()
        elif self.tileName is not None and self.date is not None:
            self.name_to_url()
            self.url_to_tile_id()

        self.safe = None

    def get_structure(self):
        self.safe = {}
        mainFolder = self.get_main_folder()
        self.safe[mainFolder] = {}

        self.safe[mainFolder][AUX_DATA] = {}
        self.safe[mainFolder][AUX_DATA][self.get_aux_data_name()] = self.tileUrl + '/auxiliary/ECMWFT'

        self.safe[mainFolder][IMG_DATA] = {}
        for band in BANDS:
            self.safe[mainFolder][IMG_DATA][self.get_img_name(band)] = self.tileUrl + '/' + band + '.jp2'
        if self.safeType == COMPACT_SAFE_TYPE:
            self.safe[mainFolder][IMG_DATA][self.get_img_name('TCI')] = self.tileUrl + '/TCI.jp2'

        self.safe[mainFolder][QI_DATA] = {}
        self.safe[mainFolder][QI_DATA][self.get_gml_name('CLOUDS')] = self.get_gml_url('CLOUDS')
        for qiType in QI_LIST:
            for band in BANDS:
                self.safe[mainFolder][QI_DATA][self.get_gml_name(qiType, band)] = self.get_gml_url(qiType, band)
        self.safe[mainFolder][QI_DATA][self.get_preview_name()] = self.tileUrl + '/preview.jp2'

        self.safe[mainFolder][self.get_tile_metadata_name()] = self.tileUrl + '/metadata.xml'

        return self.safe

    def download_structure(self):
        self.get_download_list(True)
        #print(self.downloadList)
        download.download_data(self.downloadList)

    def get_download_list(self, createFolders=False):
        if self.safe is None:
            self.get_structure()
        self.downloadList = []
        structure_recursion(self.safe, self.folder, self.downloadList, createFolders)
        return self.downloadList

    #S2A_OPER_MSI_L1C_TL_EPA__20160120T231011_A002783_T13PHS_N02.01
    #L1C_T54HVH_A009451_20170414T003551
    def tile_id_to_name(self):
        info = self.tileId.split('_')
        '''if self.safeType == OLD_SAFE_TYPE: # for old format this isn't possible
            self.tileName = info[-2].lstrip('T')
            time = info[-4]'''
        if self.safeType == COMPACT_SAFE_TYPE:
            self.tileName = info[1].lstrip('T')
            time = info[-1]
            date = [time[:4], time[4:6], time[6:8]]
            self.date = DATE_SEPARATOR.join(list(map(lambda x: x.lstrip('0'), date)))

    def name_to_url(self, index=0):
        self.tileUrl = '/'.join(MAIN_URL, 'tiles', self.name[:2], self.name[2], self.name[3:4], self.date.replace(DATE_SEPARATOR, '/'), str(index))

    # "tiles/13/P/HS/2016/1/3/0"
    def url_to_name(self):
        info = self.tileUrl.split('/')
        self.tileName = ''.join(info[-7: -4])
        self.date = DATE_SEPARATOR.join(info[-4: -1])

    # S2A_OPER_MSI_L1C_TL_SGS__20170414T033348_A009451_T54HVH_N02.04
    # L1C_T54HVH_A009451_20170414T003551
    def url_to_tile_id(self):
        response = download.make_request(self.tileUrl + '/metadata.xml', returnData=True, verbose=False)
        tree = ElementTree.fromstring(response.content)
        self.tileId = tree[0].find('TILE_ID').text
        if self.safeType == COMPACT_SAFE_TYPE:
            info = self.tileId.split('_')
            self.tileId = '_'.join([info[3], info[-2], info[-3], self.get_sensing_time()])

    def get_sensing_time(self):
        return self.tileInfo['tiles'][0]['timestamp'].split('.')[0].replace('-', '').replace(':', '')

    def get_main_folder(self):
        return self.tileId

    def get_safe_type(self):
        if self.tileId is not None:
            if self.tileId[:2] == 'S2A':
                safeType = OLD_SAFE_TYPE
            if self.tileId[:2] == 'L1C':
                safeType = COMPACT_SAFE_TYPE
            return safeType

        if self.tileUrl is not None:
            self.url_to_name()
        if self.date is not None:
            if self.date <= TYPE_CHANGE_DATE:
                safeType = OLD_SAFE_TYPE
            else:
                safeType = COMPACT_SAFE_TYPE
        return safeType

    #S2A_OPER_MTD_L1C_TL_EPA__20160120T231011_A002783_T13PHS
    def get_tile_metadata_name(self):
        if self.safeType == OLD_SAFE_TYPE:
            name = edit_name(self.tileId, 'MTD', deleteEnd=True)
        if self.safeType == COMPACT_SAFE_TYPE:
            name = 'MTD_TL'
        return name + '.xml'

    # aux: S2A_OPER_AUX_ECMWFT_EPA__20160120T231011_V20160103T150000_20160104T030000
    # tile:S2A_OPER_MSI_L1C_TL_EPA__20160120T231011_A002783_T13PHS_N02.01
    # datastrip: S2A_OPER_MSI_L1C_DS_EPA__20160120T231011_S20160103T171621_N02.01
    def get_aux_data_name(self):
        if self.safeType == OLD_SAFE_TYPE:
            return 'AUX_ECMWFT' # no idea how to get S2A_OPER_AUX_ECMWFT_EPA__20160120T231011_V20160103T150000_20160104T030000
        if self.safeType == COMPACT_SAFE_TYPE:
            return 'AUX_ECMWFT'

    # S2A_OPER_MSI_L1C_TL_EPA__20160120T231011_A002783_T13PHS_B01
    # T54HVH_20170414T003551_B01
    def get_img_name(self, band):
        if self.safeType == OLD_SAFE_TYPE:
            info = self.tileId.split('_')
            info[-1] = band
            name = '_'.join(info)
        if self.safeType == COMPACT_SAFE_TYPE:
            info = self.tileId.split('_')
            name = '_'.join([info[1], info[3], band])
        return name + '.jp2'

    # S2A_OPER_MSK_DEFECT_EPA__20160120T231011_A002783_T13PHS_B01_MSIL1C
    # MSK_CLOUDS_B00
    def get_gml_name(self, qiType, band='B00'):
        if self.safeType == OLD_SAFE_TYPE:
            name = edit_name(self.tileId, 'MSK', deleteEnd=True)
            name = name.replace('L1C_TL', qiType)
            name += '_' + band + '_MSIL1C'
        if self.safeType == COMPACT_SAFE_TYPE:
            name = 'MSK_' + qiType + '_' + band
        return name + '.gml'

    def get_gml_url(self, qiType, band='B00'):
        return self.tileUrl + '/qi/MSK_' + qiType + '_' + band + '.gml'

    # S2A_OPER_PVI_L1C_TL_EPA__20160120T231011_A002783_T13PHS.jp2
    # T54HVH_20170414T003551_PVI.jp2
    def get_preview_name(self):
        if self.safeType == OLD_SAFE_TYPE:
            name = edit_name(self.tileId, 'PVI', deleteEnd=True)
        if self.safeType == COMPACT_SAFE_TYPE:
            info = self.tileId.split('_')
            name = '_'.join([info[1], info[3], 'PVI'])
        return name + '.jp2'

def edit_name(name, code, addCode=None, deleteEnd=False):
    info = name.split('_')
    info[2] = code
    if addCode is not None:
        info[3] = addCode
    if deleteEnd:
        info.pop()
    return '_'.join(info)

def structure_recursion(struct, folder, downloadList, createFolders):
    if createFolders and len(struct) == 0:
        download.set_folder(folder)
    for name in struct.keys():
        subfolder = folder + '/' + name
        if not isinstance(struct[name], dict):
            downloadList.append((struct[name], subfolder))
        else:
            structure_recursion(struct[name], subfolder, downloadList, createFolders)

### Public functions:

def get_safe_format(productId, folder=DEFAULT_DATA_LOCATION):
    safeProduct = SafeProduct(productId, folder)
    return safeProduct.get_structure()

def download_safe_format(productId, folder=DEFAULT_DATA_LOCATION, redownload=REDOWNLOAD, threadedDownload=THREADED_DOWNLOAD):
    safeProduct = SafeProduct(productId, folder)
    print('ok')
    safeProduct.download_structure(redownload=redownload, threadedDownload=threadedDownload)

if __name__ == '__main__':
    download_safe_format('S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_20160103T171947')
    download_safe_format('S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551')

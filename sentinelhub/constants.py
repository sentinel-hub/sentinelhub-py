"""
Module with enum constants and utm utils
"""
import itertools as it
import mimetypes
import utm

from enum import Enum


mimetypes.add_type('application/json', '.json')


class DataSource(Enum):
    """ Enum constant class for data source services

    Supported types are WMS, WCS, AWS
    """
    WMS = 'wms'
    WCS = 'wcs'
    AWS = 'aws'


class _Direction(Enum):
    """ Enum constant class to encode NORTH/SOUTH direction """
    NORTH = 'N'
    SOUTH = 'S'


def _get_utm_code(zone, direction):
    """ Get UTM code given a zone and direction

    Direction is encoded as NORTH=6, SOUTH=7, while zone is the UTM zone number zero-padded.
    For instance, the code 32604 is returned for zone number 4, north direction.

    :param zone: UTM zone number
    :type zone: int
    :param direction: Direction enum type
    :type direction: Enum
    :return: UTM code
    :rtype: str
    """
    dir_dict = {_Direction.NORTH: '6', _Direction.SOUTH: '7'}
    return '{}{}{}'.format('32', dir_dict[direction], str(zone).zfill(2))


def _get_utm_name_value_pair(zone, direction=_Direction.NORTH):
    """ Get name and code for UTM coordinates

    :param zone: UTM zone number
    :type zone: int
    :param direction: Direction enum type
    :type direction: Enum, optional (default=NORTH)
    :return: Name and code of UTM coordinates
    :rtype: str, str
    """
    name = 'UTM_{}{}'.format(zone, direction.value)
    epsg = _get_utm_code(zone, direction)
    return name, epsg


class _BaseCRS(Enum):
    """ Coordinate Reference System enumerate class """
    def __str__(self):
        return self.ogc_string(self)

    @staticmethod
    def get_utm_from_wgs84(lat, lng):
        """ Convert from WGS84 to UTM coordinate system

        :param lat: Latitude
        :type lat: float
        :param lng: Longitude
        :type lng: float
        :return: UTM coordinates
        :rtype: tuple
        """
        _, _, zone, _ = utm.from_latlon(lat, lng)
        direction = 'N' if lat >= 0 else 'S'
        return CRS['UTM_{}{}'.format(str(zone), direction)]

    @staticmethod
    def ogc_string(crs):
        """ Returns a string of the form authority:id representing the CRS.

        :param crs: An enum constant representing a coordinate reference system.
        :type crs: Enum constant
        :return: A string representation of the CRS.
        :rtype: str
        """
        return 'EPSG:' + CRS(crs).value

    @classmethod
    def has_value(cls, value):
        """ Tests whether CRS contains a constant defined with string `value`.

        :param value: The string representation of the enum constant.
        :type value: str
        :return: `True` if there exists a constant with string value `value`, `False` otherwise
        :rtype: bool
        """
        return any(value == item.value for item in cls)


# Look-up class with possible combinations of UTM zone and direction
CRS = _BaseCRS("CRS", dict(
    [_get_utm_name_value_pair(zone, direction) for zone, direction in it.product(range(1, 65), _Direction)] +
    [('WGS84', '4326'), ('POP_WEB', '3857')]
))


class CustomUrlParam(Enum):
    """ Enum class to represent supported custom url parameters of OGC services

    Supported parameters are `AtmFilter`, `EvalScriptUrl`, `Preview`, `Quality`, `Upsampling`,
    `Downsampling`.

    See http://sentinel-hub.com/develop/documentation/api/custom-url-parameters for more information.
    """
    ATMFILTER = 'AtmFilter'
    EVALSCRIPTURL = 'EvalScriptUrl'
    PREVIEW = 'Preview'
    QUALITY = 'Quality'
    UPSAMPLING = 'Upsampling'
    DOWNSAMPLING = 'Downsampling'

    @classmethod
    def has_value(cls, value):
        """ Tests whether CustomUrlParam contains a constant defined with a string `value`

        :param value: The string representation of the enum constant
        :type value: str
        :return: `True` if there exists a constant with a string value `value`, `False` otherwise
        :rtype: bool
        """
        return any(value.lower() == item.value.lower() for item in cls)

    @staticmethod
    def get_string(param):
        """ Get custom url parameter name as string

        :param param: CustomUrlParam enum constant
        :type param: Enum constant
        :return: String describing the file format
        :rtype: str
        """
        return param.value


class MimeType(Enum):
    """ Enum class to represent supported image file formats

    Supported file formats are TIFF, TIFF 32-bit float, PNG, JPEG, JPEG2000, JSON, CSV, ZIP, HDF5, XML, GML, RAW

    """
    TIFF_d32f = 'tiff;depth32f'
    TIFF = 'tiff'
    PNG = 'png'
    JPG = 'jpg'
    JP2 = 'jp2'
    JSON = 'json'
    CSV = 'csv'
    ZIP = 'zip'
    HDF = 'hdf'
    XML = 'xml'
    GML = 'gml'
    TXT = 'txt'
    RAW = 'raw'
    SAFE = 'safe'
    REQUESTS_RESPONSE = 'response'  # http://docs.python-requests.org/en/master/api/#requests.Response

    @staticmethod
    def canonical_extension(fmt_ext):
        """ Canonical extension of file format extension

        Converts the format extension fmt_ext into the canonical extension for that format. For example,
        canonical_extension('tif') = 'tiff'. Here we agree that the canonical extension for format F is F.value

        :param fmt_ext: A string representing an extension (e.g. 'txt', 'png', etc.)
        :type fmt_ext: str
        :return: The canonical form of the extension (e.g. if fmt_ext='tif' then we return 'tiff')
        :rtype: str
        """
        if MimeType.has_value(fmt_ext):
            return fmt_ext
        try:
            return {
                'tif': MimeType.TIFF.value,
                'jpeg': MimeType.JPG.value,
                'hdf5': MimeType.HDF.value,
                'h5': MimeType.HDF.value
            }[fmt_ext]
        except KeyError:
            raise ValueError('Data format .{} is not supported'.format(fmt_ext))

    @staticmethod
    def is_image_format(value):
        """ Checks whether file format is an image format

        :param value: File format
        :type value: str
        :return: `True` if file is in image format, `False` otherwise
        :rtype: bool
        """
        return value in frozenset([MimeType.TIFF, MimeType.TIFF_d32f, MimeType.PNG, MimeType.JP2, MimeType.JPG])

    @classmethod
    def has_value(cls, value):
        """ Tests whether MimeType contains a constant defined with string `value`

        :param value: The string representation of the enum constant
        :type value: str
        :return: `True` if there exists a constant with string value `value`, `False` otherwise
        :rtype: bool
        """
        return any(value == item.value for item in cls)

    @staticmethod
    def get_string(fmt):
        """ Get file format as string

        :param fmt: MimeType enum constant
        :type fmt: Enum constant
        :return: String describing the file format
        :rtype: str
        """
        if fmt is MimeType.TIFF_d32f:
            return 'image/tiff;depth=32f'
        elif fmt is MimeType.JP2:
            return 'image/jpeg2000'
        elif fmt is MimeType.RAW or fmt is MimeType.REQUESTS_RESPONSE:
            return fmt.value
        return mimetypes.types_map['.' + fmt.value]


class RequestType(Enum):
    """ Enum constant class for GET/POST request type """
    GET = 'GET'
    POST = 'POST'


class OgcConstants:
    """ Initialisation of constants used by OGC request.

        Constants are LATEST
    """
    LATEST = 'latest'


class AwsConstants:
    """ Initialisation of constants used by AWS classes

        Constants are BANDS, TILE_INFO, PRODUCT_INFO, METADATA, PREVIEW, QI_LIST, AUX_DATA, DATASTRIP, GRANULE, HTML,
        INFO, QI_DATA, IMG_DATA, INSPIRE, MANIFEST, DATASTRIP_FILE, FILEFORMATS, PRODUCT_METAFILES, TILE_FILES
    """
    BANDS = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12']
    TILE_INFO = 'tileInfo'
    PRODUCT_INFO = 'productInfo'
    METADATA = 'metadata'
    PREVIEW = 'preview'
    QI_LIST = ['DEFECT', 'DETFOO', 'NODATA', 'SATURA', 'TECQUA']
    AUX_DATA = 'AUX_DATA'
    DATASTRIP = 'DATASTRIP'
    GRANULE = 'GRANULE'
    HTML = 'HTML'
    INFO = 'rep_info'
    QI_DATA = 'QI_DATA'
    IMG_DATA = 'IMG_DATA'
    INSPIRE = 'inspire'
    MANIFEST = 'manifest'
    DATASTRIP_FILE = 'datastrip/*/metadata'

    FILE_FORMATS = {METADATA: MimeType.XML, PREVIEW: MimeType.JPG, TILE_INFO: MimeType.JSON,
                    PRODUCT_INFO: MimeType.JSON, 'TCI': MimeType.JP2, INSPIRE: MimeType.XML,
                    'auxiliary/ECMWFT': MimeType.RAW, MANIFEST: MimeType.SAFE, 'qi/MSK_CLOUDS_B00': MimeType.GML,
                    DATASTRIP_FILE: MimeType.XML}
    for band in BANDS:
        FILE_FORMATS[band] = MimeType.JP2
        FILE_FORMATS['preview/' + band] = MimeType.JP2
        for quality_indicator in QI_LIST:
            FILE_FORMATS['qi/MSK_{}_{}'.format(quality_indicator, band)] = MimeType.GML

    PRODUCT_METAFILES = [METADATA, PRODUCT_INFO, INSPIRE, MANIFEST, DATASTRIP_FILE]
    TILE_FILES = []
    for filename in FILE_FORMATS:
        if filename not in [INSPIRE, MANIFEST, DATASTRIP_FILE]:
            TILE_FILES.append(filename)


class EsaSafeType(Enum):
    """ Enum constants class for ESA .SAFE type.

     Types are OLD_SAFE_TYPE or COMPACT_SAFE_TYPE
     """
    OLD_SAFE_TYPE = 'old_type'
    COMPACT_SAFE_TYPE = 'compact_type'

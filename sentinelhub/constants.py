"""
Module defining constants and enumerate types used in the package
"""
import functools
import itertools as it
import mimetypes
import re
import warnings
from enum import Enum, EnumMeta

import utm
import pyproj
from aenum import extend_enum

from .exceptions import SHUserWarning
from ._version import __version__


class PackageProps:
    """ Class for obtaining package properties. Currently it supports obtaining package version."""

    @staticmethod
    def get_version():
        """ Returns package version

        :return: package version
        :rtype: str
        """
        return __version__


class ServiceUrl:
    """ Most commonly used Sentinel Hub service URLs
    """
    MAIN = 'https://services.sentinel-hub.com'
    USWEST = 'https://services-uswest2.sentinel-hub.com'
    CREODIAS = 'https://creodias.sentinel-hub.com'
    EOCLOUD = 'http://services.eocloud.sentinel-hub.com'
    MUNDI = 'https://shservices.mundiwebservices.com'


class ServiceType(Enum):
    """ Enum constant class for type of service

    Supported types are WMS, WCS, WFS, AWS, IMAGE
    """
    WMS = 'wms'
    WCS = 'wcs'
    WFS = 'wfs'
    AWS = 'aws'
    IMAGE = 'image'
    FIS = 'fis'
    PROCESSING_API = 'processing'


class CRSMeta(EnumMeta):
    """ Metaclass used for building CRS Enum class
    """
    _UNSUPPORTED_CRS = pyproj.CRS(4326)

    def __new__(mcs, cls, bases, classdict):
        """ This is executed at the beginning of runtime when CRS class is created
        """
        for direction, direction_value in [('N', '6'), ('S', '7')]:
            for zone in range(1, 61):
                classdict[f'UTM_{zone}{direction}'] = f'32{direction_value}{zone:02}'

        return super().__new__(mcs, cls, bases, classdict)

    def __call__(cls, crs_value, *args, **kwargs):
        """ This is executed whenever CRS('something') is called
        """
        # pylint: disable=signature-differs
        crs_value = cls._parse_crs(crs_value)

        if isinstance(crs_value, str) and not cls.has_value(crs_value) and crs_value.isdigit() and len(crs_value) >= 4:
            crs_name = f'EPSG_{crs_value}'
            extend_enum(cls, crs_name, crs_value)

        return super().__call__(crs_value, *args, **kwargs)

    @staticmethod
    def _parse_crs(value):
        """ Method for parsing different inputs representing the same CRS enum. Examples:

        - 4326
        - 'EPSG:3857'
        - {'init': 32633}
        - geojson['crs']['properties']['name'] string (urn:ogc:def:crs:...)
        - pyproj.CRS(32743)
        """
        if isinstance(value, dict) and 'init' in value:
            value = value['init']
        if isinstance(value, pyproj.CRS):
            if value == CRSMeta._UNSUPPORTED_CRS:
                message = 'sentinelhub-py supports only WGS 84 coordinate reference system with ' \
                          'coordinate order lng-lat. Given pyproj.CRS(4326) has coordinate order lat-lng. Be careful ' \
                          'to use the correct order of coordinates.'
                warnings.warn(message, category=SHUserWarning)

            epsg_code = value.to_epsg()
            if epsg_code is not None:
                return str(epsg_code)

            error_message = f'Failed to determine an EPSG code of the given CRS:\n{repr(value)}'
            maybe_epsg = value.to_epsg(min_confidence=0)
            if maybe_epsg is not None:
                error_message = f'{error_message}\nIt might be EPSG {maybe_epsg} but pyproj is not confident ' \
                                'enough.'
            raise ValueError(error_message)

        if isinstance(value, int):
            return str(value)
        if isinstance(value, str):
            if 'urn:ogc:def:crs' in value.lower():
                crs_template = re.compile(r'urn:ogc:def:crs:.+::(?P<code>.+)', re.IGNORECASE)
                value = crs_template.match(value).group("code")
            if value.upper() == 'CRS84':
                return '4326'
            return value.lower().strip('epsg: ')
        return value


class CRS(Enum, metaclass=CRSMeta):
    """ Coordinate Reference System enumerate class

    Available CRS constants are WGS84, POP_WEB (i.e. Popular Web Mercator) and constants in form UTM_<zone><direction>,
    where zone is an integer from [1, 60] and direction is either N or S (i.e. northern or southern hemisphere)
    """
    WGS84 = '4326'
    POP_WEB = '3857'
    #: UTM enum members are defined in CRSMeta.__new__

    def __str__(self):
        """ Method for casting CRS enum into string
        """
        return self.ogc_string()

    def __repr__(self):
        """ Method for retrieving CRS enum representation
        """
        return f"CRS('{self.value}')"

    @classmethod
    def has_value(cls, value):
        """ Tests whether CRS contains a constant defined with string `value`.

        :param value: The string representation of the enum constant.
        :type value: str
        :return: `True` if there exists a constant with string value `value`, `False` otherwise
        :rtype: bool
        """
        return value in cls._value2member_map_

    @property
    def epsg(self):
        """ EPSG code property

        :return: EPSG code of given CRS
        :rtype: int
        """
        return int(self.value)

    def ogc_string(self):
        """ Returns a string of the form authority:id representing the CRS.

        :param self: An enum constant representing a coordinate reference system.
        :type self: CRS
        :return: A string representation of the CRS.
        :rtype: str
        """
        return f'EPSG:{CRS(self).value}'

    @property
    def opengis_string(self):
        """ Returns an URL to OGC webpage where the CRS is defined

        :return: An URL with CRS definition
        :rtype: str
        """
        return f'http://www.opengis.net/def/crs/EPSG/0/{self.epsg}'

    def is_utm(self):
        """ Checks if crs is one of the 64 possible UTM coordinate reference systems.

        :param self: An enum constant representing a coordinate reference system.
        :type self: CRS
        :return: `True` if crs is UTM and `False` otherwise
        :rtype: bool
        """
        return self.name.startswith('UTM')

    @functools.lru_cache(maxsize=5)
    def projection(self):
        """ Returns a projection in form of pyproj class. For better time performance it will cache results of
        5 most recently used CRS classes.

        :return: pyproj projection class
        :rtype: pyproj.Proj
        """
        return pyproj.Proj(self._get_pyproj_projection_def(), preserve_units=True)

    @functools.lru_cache(maxsize=5)
    def pyproj_crs(self):
        """ Returns a pyproj CRS class. For better time performance it will cache results of
        5 most recently used CRS classes.

        :return: pyproj CRS class
        :rtype: pyproj.CRS
        """
        return pyproj.CRS(self._get_pyproj_projection_def())

    @functools.lru_cache(maxsize=10)
    def get_transform_function(self, other, always_xy=True):
        """ Returns a function for transforming geometrical objects from one CRS to another. The function will support
        transformations between any objects that pyproj supports.
        For better time performance this method will cache results of 10 most recently used pairs of CRS classes.

        :param self: Initial CRS
        :type self: CRS
        :param other: Target CRS
        :type other: CRS
        :param always_xy: Parameter that is passed to `pyproj.Transformer` object and defines axis order for
            transformation. The default value `True` is in most cases the correct one.
        :type always_xy: bool
        :return: A projection function obtained from pyproj package
        :rtype: function
        """
        return pyproj.Transformer.from_proj(self.projection(), other.projection(), always_xy=always_xy).transform

    @staticmethod
    def get_utm_from_wgs84(lng, lat):
        """ Convert from WGS84 to UTM coordinate system

        :param lng: Longitude
        :type lng: float
        :param lat: Latitude
        :type lat: float
        :return: UTM coordinates
        :rtype: tuple
        """
        _, _, zone, _ = utm.from_latlon(lat, lng)
        direction = 'N' if lat >= 0 else 'S'
        return CRS[f'UTM_{zone}{direction}']

    def _get_pyproj_projection_def(self):
        """ Returns a pyproj crs definition

        For WGS 84 it ensures lng-lat order
        """
        return '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs' if self is CRS.WGS84 else self.ogc_string()


class CustomUrlParam(Enum):
    """ Enum class to represent supported custom url parameters of OGC services

    Supported parameters are `SHOWLOGO`, `EVALSCRIPT`, `EVALSCRIPTURL`, `PREVIEW`, `QUALITY`, `UPSAMPLING`,
    `DOWNSAMPLING`, `GEOMETRY` and `WARNINGS`.

    See http://sentinel-hub.com/develop/documentation/api/custom-url-parameters and
    https://www.sentinel-hub.com/develop/documentation/api/ogc_api/wms-parameters for more information.
    """
    SHOWLOGO = 'ShowLogo'
    EVALSCRIPT = 'EvalScript'
    EVALSCRIPTURL = 'EvalScriptUrl'
    PREVIEW = 'Preview'
    QUALITY = 'Quality'
    UPSAMPLING = 'Upsampling'
    DOWNSAMPLING = 'Downsampling'
    GEOMETRY = 'Geometry'
    MINQA = 'MinQA'

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


class HistogramType(Enum):
    """ Enum class for types of histogram supported by Sentinel Hub FIS service

    Supported histogram types are EQUALFREQUENCY, EQUIDISTANT and STREAMING
    """
    EQUALFREQUENCY = 'equalfrequency'
    EQUIDISTANT = 'equidistant'
    STREAMING = 'streaming'


class MimeType(Enum):
    """ Enum class to represent supported file formats

    Supported file formats are TIFF 8-bit, TIFF 16-bit, TIFF 32-bit float, PNG, JPEG, JPEG2000, JSON, CSV, ZIP, HDF5,
    XML, GML, RAW
    """
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
    TAR = 'tar'
    RAW = 'raw'
    SAFE = 'safe'

    @property
    def extension(self):
        """ Returns file extension of the MimeType object

        :returns: A file extension string
        :rtype: str
        """
        return self.value

    @staticmethod
    def from_string(mime_type_str):
        """ Parses mime type from a file extension string

        :param mime_type_str: A file extension string
        :type mime_type_str: str
        :return: A mime type enum
        :rtype: MimeType
        """

        # These two cases are handled seperately due to issues with python 3.6
        if mime_type_str == 'image/jpeg':
            return MimeType.JPG
        if mime_type_str == 'text/plain':
            return MimeType.TXT

        guessed_extension = mimetypes.guess_extension(mime_type_str)
        if guessed_extension:
            mime_type_str = guessed_extension.strip('.')
        else:
            mime_type_str = mime_type_str.split('/')[-1]

        if MimeType.has_value(mime_type_str):
            return MimeType(mime_type_str)

        try:
            return {
                'tif': MimeType.TIFF,
                'jpeg': MimeType.JPG,
                'hdf5': MimeType.HDF,
                'h5': MimeType.HDF
            }[mime_type_str]
        except KeyError as exception:
            raise ValueError(f'Data format {mime_type_str} is not supported') from exception

    def is_image_format(self):
        """ Checks whether file format is an image format

        Example: ``MimeType.PNG.is_image_format()`` or ``MimeType.is_image_format(MimeType.PNG)``

        :param self: File format
        :type self: MimeType
        :return: `True` if file is in image format, `False` otherwise
        :rtype: bool
        """
        return self in frozenset([MimeType.TIFF, MimeType.PNG, MimeType.JP2, MimeType.JPG])

    def is_api_format(self):
        """ Checks if mime type is supported by Sentinel Hub API

        :return: True if API supports this format and False otherwise
        :rtype: bool
        """
        return self in frozenset([MimeType.JPG, MimeType.PNG, MimeType.TIFF, MimeType.JSON])

    @classmethod
    def has_value(cls, value):
        """ Tests whether MimeType contains a constant defined with string ``value``

        :param value: The string representation of the enum constant
        :type value: str
        :return: `True` if there exists a constant with string value ``value``, `False` otherwise
        :rtype: bool
        """
        return value in cls._value2member_map_

    def get_string(self):
        """ Get file format as string

        :return: String describing the file format
        :rtype: str
        """
        if self is MimeType.TAR:
            return 'application/x-tar'
        if self is MimeType.JSON:
            return 'application/json'
        if self is MimeType.JP2:
            return 'image/jpeg2000'
        if self is MimeType.XML:
            return 'text/xml'
        if self is MimeType.RAW:
            return self.value
        return mimetypes.types_map['.' + self.value]

    def get_expected_max_value(self):
        """ Returns max value of image `MimeType` format and raises an error if it is not an image format

        :return: A maximum value of specified image format
        :rtype: int or float
        :raises: ValueError
        """
        try:
            return {
                MimeType.TIFF: 65535,
                MimeType.PNG: 255,
                MimeType.JPG: 255,
                MimeType.JP2: 10000
            }[self]
        except KeyError as exception:
            raise ValueError(f'Type {self} is not supported by this method') from exception


class RequestType(Enum):
    """ Enum constant class for GET/POST request type """
    GET = 'GET'
    POST = 'POST'
    DELETE = 'DELETE'
    PUT = 'PUT'
    PATCH = 'PATCH'


class SHConstants:
    """ Initialisation of constants used by OGC request.

        Constants are LATEST
    """
    LATEST = 'latest'
    HEADERS = {'User-Agent': f'sentinelhub-py/v{PackageProps.get_version()}'}


class AwsConstants:
    """ Initialisation of every constant used by AWS classes

    For each supported data collection it contains lists of all possible bands and all possible metadata files:

        - S2_L1C_BANDS and S2_L1C_METAFILES
        - S2_L2A_BANDS and S2_L2A_METAFILES

    It also contains dictionary of all possible files and their formats: AWS_FILES
    """
    # General constants:
    SOURCE_ID_LIST = ['L1C', 'L2A']
    TILE_INFO = 'tileInfo'
    PRODUCT_INFO = 'productInfo'
    METADATA = 'metadata'
    PREVIEW = 'preview'
    PREVIEW_JP2 = 'preview*'
    QI_LIST = ['DEFECT', 'DETFOO', 'NODATA', 'SATURA', 'TECQUA']
    QI_MSK_CLOUD = 'qi/MSK_CLOUDS_B00'
    AUX_DATA = 'AUX_DATA'
    DATASTRIP = 'DATASTRIP'
    GRANULE = 'GRANULE'
    HTML = 'HTML'
    INFO = 'rep_info'
    QI_DATA = 'QI_DATA'
    IMG_DATA = 'IMG_DATA'
    INSPIRE = 'inspire'
    MANIFEST = 'manifest'
    TCI = 'TCI'
    PVI = 'PVI'
    ECMWFT = 'auxiliary/ECMWFT'
    AUX_ECMWFT = 'auxiliary/AUX_ECMWFT'
    DATASTRIP_METADATA = 'datastrip/*/metadata'

    # More constants about L2A
    AOT = 'AOT'
    WVP = 'WVP'
    SCL = 'SCL'
    VIS = 'VIS'
    L2A_MANIFEST = 'L2AManifest'
    REPORT = 'report'
    GIPP = 'auxiliary/GIP_TL'
    FORMAT_CORRECTNESS = 'FORMAT_CORRECTNESS'
    GENERAL_QUALITY = 'GENERAL_QUALITY'
    GEOMETRIC_QUALITY = 'GEOMETRIC_QUALITY'
    RADIOMETRIC_QUALITY = 'RADIOMETRIC_QUALITY'
    SENSOR_QUALITY = 'SENSOR_QUALITY'
    QUALITY_REPORTS = [FORMAT_CORRECTNESS, GENERAL_QUALITY, GEOMETRIC_QUALITY, RADIOMETRIC_QUALITY, SENSOR_QUALITY]
    CLASS_MASKS = ['SNW', 'CLD']
    R10m = 'R10m'
    R20m = 'R20m'
    R60m = 'R60m'
    RESOLUTIONS = [R10m, R20m, R60m]
    S2_L2A_BAND_MAP = {R10m: ['B02', 'B03', 'B04', 'B08', AOT, TCI, WVP],
                       R20m: ['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B8A', 'B11', 'B12', AOT, SCL, TCI, VIS, WVP],
                       R60m: ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B8A', 'B09', 'B11', 'B12', AOT, SCL,
                              TCI, WVP]}

    # Order of elements in following lists is important
    # Sentinel-2 L1C products:
    S2_L1C_BANDS = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12']
    S2_L1C_METAFILES = [PRODUCT_INFO, TILE_INFO, METADATA, INSPIRE, MANIFEST, DATASTRIP_METADATA] +\
                       [PREVIEW, PREVIEW_JP2, TCI] +\
                       [f'{preview}/{band}' for preview, band in it.zip_longest([], S2_L1C_BANDS, fillvalue=PREVIEW)] +\
                       [QI_MSK_CLOUD] +\
                       [f'qi/MSK_{qi}_{band}' for qi, band in it.product(QI_LIST, S2_L1C_BANDS)] + \
                       [f'qi/{qi_report}' for qi_report in [FORMAT_CORRECTNESS, GENERAL_QUALITY,
                                                            GEOMETRIC_QUALITY, SENSOR_QUALITY]] +\
                       [ECMWFT]

    # Sentinel-2 L2A products:
    S2_L2A_BANDS = [f'{resolution}/{band}' for resolution, band_list in sorted(S2_L2A_BAND_MAP.items())
                    for band in band_list]
    S2_L2A_METAFILES = [PRODUCT_INFO, TILE_INFO, METADATA, INSPIRE, MANIFEST, L2A_MANIFEST, REPORT,
                        DATASTRIP_METADATA] + [f'datastrip/*/qi/{qi_report}' for qi_report in QUALITY_REPORTS] +\
                       [f'qi/{source_id}_PVI' for source_id in SOURCE_ID_LIST] +\
                       [f'qi/{mask}_{res.lstrip("R")}' for mask, res in it.product(CLASS_MASKS, [R20m, R60m])] +\
                       [f'qi/MSK_{qi}_{band}' for qi, band in it.product(QI_LIST, S2_L1C_BANDS)] +\
                       [QI_MSK_CLOUD] +\
                       [f'qi/{qi_report}' for qi_report in QUALITY_REPORTS] +\
                       [ECMWFT, AUX_ECMWFT, GIPP]

    # Product files with formats:
    PRODUCT_FILES = {**{PRODUCT_INFO: MimeType.JSON,
                        METADATA: MimeType.XML,
                        INSPIRE: MimeType.XML,
                        MANIFEST: MimeType.SAFE,
                        L2A_MANIFEST: MimeType.XML,
                        REPORT: MimeType.XML,
                        DATASTRIP_METADATA: MimeType.XML},
                     **{f'datastrip/*/qi/{qi_report}': MimeType.XML for qi_report in QUALITY_REPORTS}}
    # Tile files with formats:
    TILE_FILES = {**{TILE_INFO: MimeType.JSON,
                     PRODUCT_INFO: MimeType.JSON,
                     METADATA: MimeType.XML,
                     PREVIEW: MimeType.JPG,
                     PREVIEW_JP2: MimeType.JP2,
                     TCI: MimeType.JP2,
                     QI_MSK_CLOUD: MimeType.GML,
                     ECMWFT: MimeType.RAW,
                     AUX_ECMWFT: MimeType.RAW,
                     GIPP: MimeType.XML},
                  **{f'qi/{qi_report}': MimeType.XML for qi_report in QUALITY_REPORTS},
                  **{f'{preview}/{band}': MimeType.JP2
                     for preview, band in it.zip_longest([], S2_L1C_BANDS, fillvalue=PREVIEW)},
                  **{f'qi/MSK_{qi}_{band}': MimeType.GML for qi, band in it.product(QI_LIST, S2_L1C_BANDS)},
                  **{band: MimeType.JP2 for band in S2_L1C_BANDS},
                  **{band: MimeType.JP2 for band in S2_L2A_BANDS},
                  **{f'qi/{source_id}_PVI': MimeType.JP2 for source_id in SOURCE_ID_LIST},
                  **{f'qi/{mask}_{res.lstrip("R")}': MimeType.JP2 for mask, res in it.product(CLASS_MASKS,
                                                                                              [R20m, R60m])}}

    # All files joined together
    AWS_FILES = {**PRODUCT_FILES,
                 **{filename.split('/')[-1]: data_format for filename, data_format in PRODUCT_FILES.items()},
                 **TILE_FILES,
                 **{filename.split('/')[-1]: data_format for filename, data_format in TILE_FILES.items()}}


class EsaSafeType(Enum):
    """ Enum constants class for ESA .SAFE type.

     Types are OLD_TYPE and COMPACT_TYPE
    """
    OLD_TYPE = 'old_type'
    COMPACT_TYPE = 'compact_type'

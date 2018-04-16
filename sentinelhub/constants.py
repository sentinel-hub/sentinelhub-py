"""
Module with enum constants and utm utils
"""

# pylint: disable=invalid-name

import itertools as it
import mimetypes
import utm
import os.path
from pyproj import Proj
from enum import Enum, EnumMeta


mimetypes.add_type('application/json', '.json')


class PackageProps:
    """ Class for obtaining package properties. Currently it supports obtaining package version."""

    @staticmethod
    def get_version():
        for line in open(os.path.join(os.path.dirname(__file__), '__init__.py')):
            if line.find("__version__") >= 0:
                version = line.split("=")[1].strip()
                version = version.strip('"').strip("'")
        return version


class ServiceType(Enum):
    """ Enum constant class for type of service

    Supported types are WMS, WCS, WFS, AWS
    """
    WMS = 'wms'
    WCS = 'wcs'
    WFS = 'wfs'
    AWS = 'aws'


class _DataSourceMeta(EnumMeta):
    """ EnumMeta class for `DataSource` Enum class
    """
    def __iter__(cls):
        return (member for name, member in cls._member_map_.items() if isinstance(member.value, tuple))

    def __len__(cls):
        return len(list(cls.__iter__()))


class DataSource(Enum, metaclass=_DataSourceMeta):
    """ Enum constant class for types of satellite data

    Supported types are SENTINEL2_L1C, SENTINEL2_L2A, LANDSAT8, SENTINEL1_IW, SENTINEL1_EW, SENTINEL1_EW_SH, DEM, MODIS
    """
    class Source(Enum):
        """
        Types of satellite sources
        """
        SENTINEL2 = 'Sentinel-2'
        SENTINEL1 = 'Sentinel-1'
        LANDSAT8 = 'Landsat 8'
        MODIS = 'MODIS'
        DEM = 'Mapzen DEM'

    class ProcessingLevel(Enum):
        """
        Types of processing level
        """
        L1C = 'L1C'
        L2A = 'L2A'
        GRD = 'GRD'
        MCD43A4 = 'MCD43A4'

    class Acquisition(Enum):
        """
        Types of Sentinel-1 acquisition
        """
        IW = 'IW'
        EW = 'EW'

    class Polarisation(Enum):
        """
        Types of Sentinel-1 polarisation
        """
        DV = 'VV+VH'
        DH = 'HH+HV'
        SV = 'VV'
        SH = 'HH'

    class Resolution(Enum):
        """
        Types of Sentinel-1 resolution
        """
        MEDIUM = 'medium'
        HIGH = 'high'

    class OrbitDirection(Enum):
        """
        Types of Sentinel-1 orbit direction
        """
        ASCENDING = 'ascending'
        DESCENDING = 'descending'
        BOTH = 'both'

    SENTINEL2_L1C = (Source.SENTINEL2, ProcessingLevel.L1C)
    SENTINEL2_L2A = (Source.SENTINEL2, ProcessingLevel.L2A)
    SENTINEL1_IW = (Source.SENTINEL1, ProcessingLevel.GRD, Acquisition.IW, Polarisation.DV, Resolution.HIGH,
                    OrbitDirection.BOTH)
    SENTINEL1_EW = (Source.SENTINEL1, ProcessingLevel.GRD, Acquisition.EW, Polarisation.DH, Resolution.MEDIUM,
                    OrbitDirection.BOTH)
    SENTINEL1_EW_SH = (Source.SENTINEL1, ProcessingLevel.GRD, Acquisition.EW, Polarisation.SH, Resolution.MEDIUM,
                       OrbitDirection.BOTH)
    DEM = (Source.DEM, )
    MODIS = (Source.MODIS, ProcessingLevel.MCD43A4)
    LANDSAT8 = (Source.LANDSAT8, ProcessingLevel.GRD)

    @classmethod
    def get_wfs_typename(cls, data_source):
        """ Maps data source to string identifier for WFS

        :param data_source: One of the supported data sources
        :type: DataSource
        :return: Product identifier for WFS
        :rtype: str
        """
        return {
            cls.SENTINEL2_L1C: 'S2.TILE',
            cls.SENTINEL2_L2A: 'DSS2',
            cls.SENTINEL1_IW: 'DSS3',
            cls.SENTINEL1_EW: 'DSS3',
            cls.SENTINEL1_EW_SH: 'DSS3',
            cls.DEM: 'DSS4',
            cls.MODIS: 'DSS5',
            cls.LANDSAT8: 'DSS6'
        }[data_source]

    @classmethod
    def is_sentinel1(cls, data_source):
        """Checks if source is Sentinel-1

        :param data_source: One of the supported data sources
        :type: DataSource
        :return: ``True`` if source is Sentinel-1 and ``False`` otherwise
        :rtype: bool
        """
        return data_source.value[0] is cls.Source.value.SENTINEL1

    @classmethod
    def is_timeless(cls, data_source):
        """Checks if data source is time independent

        :param data_source: One of the supported data sources
        :type: DataSource
        :return: ``True`` if data source is time independent and ``False`` otherwise
        :rtype: bool
        """
        return data_source.value[0] is cls.Source.value.DEM

    @classmethod
    def is_uswest_source(cls, data_source):
        """Checks if data source via Sentinel Hub services is available at US West server

        :param data_source: One of the supported data sources
        :type: DataSource
        :return: ``True`` if data source exists at US West server and ``False`` otherwise
        :rtype: bool
        """
        return data_source.value[0] in [cls.Source.value.LANDSAT8, cls.Source.value.MODIS, cls.Source.value.DEM]


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

    @staticmethod
    def is_utm(crs):
        """ Checks if crs is one of the 64 possible UTM coordinate reference systems.

        :param crs: An enum constant representing a coordinate reference system.
        :type crs: Enum constant
        :return: True if crs is UTM and False otherwise
        :rtype: bool
        """
        return crs.name.startswith('UTM')

    @classmethod
    def projection(cls, crs):
        """ Returns a projection in form of pyproj class

        :param crs: An enum constant representing a coordinate reference system.
        :type crs: Enum constant
        :return: pyproj projection class
        :rtype: pyproj.Proj
        """
        return Proj(init=cls.ogc_string(crs))

    @classmethod
    def has_value(cls, value):
        """ Tests whether CRS contains a constant defined with string ``value``.

        :param value: The string representation of the enum constant.
        :type value: str
        :return: ``True`` if there exists a constant with string value ``value``, ``False`` otherwise
        :rtype: bool
        """
        return any(value == item.value for item in cls)


# Look-up class with possible combinations of UTM zone and direction
CRS = _BaseCRS("CRS", dict(
    [_get_utm_name_value_pair(zone, direction) for zone, direction in it.product(range(1, 65), _Direction)] +
    [('WGS84', '4326'), ('POP_WEB', '3857')]
))


def _crs_parser(cls, value):
    """ Parses user input for class CRS

    :param cls: class object
    :param value: user input for CRS
    :type value: str, int or CRS
    """
    parsed_value = value
    if isinstance(parsed_value, int):
        parsed_value = str(parsed_value)
    if isinstance(parsed_value, str):
        parsed_value = parsed_value.strip('epsgEPSG: ')
    return super(_BaseCRS, cls).__new__(cls, parsed_value)


setattr(CRS, '__new__', _crs_parser)


class CustomUrlParam(Enum):
    """ Enum class to represent supported custom url parameters of OGC services

    Supported parameters are `ShowLogo`, `AtmFilter`, `EvalScript`, `EvalScriptUrl`, `Preview`, `Quality`, `Upsampling`,
    `Downsampling` and `Transparent`.

    See http://sentinel-hub.com/develop/documentation/api/custom-url-parameters for more information.
    """
    SHOWLOGO = 'ShowLogo'
    ATMFILTER = 'AtmFilter'
    EVALSCRIPT = 'EvalScript'
    EVALSCRIPTURL = 'EvalScriptUrl'
    PREVIEW = 'Preview'
    QUALITY = 'Quality'
    UPSAMPLING = 'Upsampling'
    DOWNSAMPLING = 'Downsampling'
    TRANSPARENT = 'Transparent'

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
        ``canonical_extension('tif') == 'tiff'``. Here we agree that the canonical extension for format F is F.value

        :param fmt_ext: A string representing an extension (e.g. ``'txt'``, ``'png'``, etc.)
        :type fmt_ext: str
        :return: The canonical form of the extension (e.g. if ``fmt_ext='tif'`` then we return ``'tiff'``)
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
        :return: ``True`` if file is in image format, ``False`` otherwise
        :rtype: bool
        """
        return value in frozenset([MimeType.TIFF, MimeType.TIFF_d32f, MimeType.PNG, MimeType.JP2, MimeType.JPG])

    @classmethod
    def has_value(cls, value):
        """ Tests whether MimeType contains a constant defined with string ``value``

        :param value: The string representation of the enum constant
        :type value: str
        :return: ``True`` if there exists a constant with string value ``value``, ``False`` otherwise
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
    HEADERS = {'User-Agent': 'sentinelhub-py/v{}'.format(PackageProps.get_version())}


class AwsConstants:
    """ Initialisation of every constant used by AWS classes

    For each supported data source it contains lists of all possible bands and all posible metadata files:

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
                       ['{}/{}'.format(preview, band) for preview, band in it.zip_longest([], S2_L1C_BANDS,
                                                                                          fillvalue=PREVIEW)] +\
                       [QI_MSK_CLOUD] +\
                       ['qi/MSK_{}_{}'.format(qi, band) for qi, band in it.product(QI_LIST, S2_L1C_BANDS)] + \
                       ['qi/{}'.format(qi_report) for qi_report in [FORMAT_CORRECTNESS, GENERAL_QUALITY,
                                                                    GEOMETRIC_QUALITY, SENSOR_QUALITY]] +\
                       [ECMWFT]

    # Sentinel-2 L2A products:
    S2_L2A_BANDS = ['{}/{}'.format(resolution, band) for resolution, band_list in sorted(S2_L2A_BAND_MAP.items())
                    for band in band_list]
    S2_L2A_METAFILES = [PRODUCT_INFO, TILE_INFO, METADATA, INSPIRE, MANIFEST, L2A_MANIFEST, REPORT,
                        DATASTRIP_METADATA] + ['datastrip/*/qi/{}'.format(qi_report)for qi_report in QUALITY_REPORTS] +\
                       ['qi/{}_PVI'.format(source_id) for source_id in SOURCE_ID_LIST] +\
                       ['qi/{}_{}'.format(mask, res.lstrip('R')) for mask, res in it.product(CLASS_MASKS,
                                                                                             [R20m, R60m])] +\
                       ['qi/MSK_{}_{}'.format(qi, band) for qi, band in it.product(QI_LIST, S2_L1C_BANDS)] +\
                       [QI_MSK_CLOUD] +\
                       ['qi/{}'.format(qi_report) for qi_report in [FORMAT_CORRECTNESS, GENERAL_QUALITY,
                                                                    GEOMETRIC_QUALITY, SENSOR_QUALITY]] +\
                       [ECMWFT, AUX_ECMWFT, GIPP]

    # Product files with formats:
    PRODUCT_FILES = {**{PRODUCT_INFO: MimeType.JSON,
                        METADATA: MimeType.XML,
                        INSPIRE: MimeType.XML,
                        MANIFEST: MimeType.SAFE,
                        L2A_MANIFEST: MimeType.XML,
                        REPORT: MimeType.XML,
                        DATASTRIP_METADATA: MimeType.XML},
                     **{'datastrip/*/qi/{}'.format(qi_report): MimeType.XML for qi_report in QUALITY_REPORTS}}
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
                  **{'qi/{}'.format(qi_report): MimeType.XML for qi_report in [FORMAT_CORRECTNESS, GENERAL_QUALITY,
                                                                               GEOMETRIC_QUALITY, SENSOR_QUALITY]},
                  **{'{}/{}'.format(preview, band): MimeType.JP2
                     for preview, band in it.zip_longest([], S2_L1C_BANDS, fillvalue=PREVIEW)},
                  **{'qi/MSK_{}_{}'.format(qi, band): MimeType.GML for qi, band in it.product(QI_LIST, S2_L1C_BANDS)},
                  **{band: MimeType.JP2 for band in S2_L1C_BANDS},
                  **{band: MimeType.JP2 for band in S2_L2A_BANDS},
                  **{'qi/{}_PVI'.format(source_id): MimeType.JP2 for source_id in SOURCE_ID_LIST},
                  **{'qi/{}_{}'.format(mask, res.lstrip('R')): MimeType.JP2 for mask, res in it.product(CLASS_MASKS,
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

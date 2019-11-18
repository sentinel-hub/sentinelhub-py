"""
Module defining constants and enumerate types used in the package
"""
import re
import functools
import itertools as it
import mimetypes
from enum import Enum, EnumMeta
from aenum import extend_enum

import utm
import pyproj

from .config import SHConfig
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


class _Source(Enum):
    """
    Types of satellite sources
    """
    SENTINEL2 = 'Sentinel-2'
    SENTINEL1 = 'Sentinel-1'
    LANDSAT8 = 'Landsat 8'
    MODIS = 'MODIS'
    DEM = 'Mapzen DEM'
    LANDSAT5 = 'Landsat 5'
    LANDSAT7 = 'Landsat 7'
    SENTINEL3 = 'Sentinel-3'
    SENTINEL5P = 'Sentinel-5P'
    ENVISAT_MERIS = 'Envisat Meris'


class _ProcessingLevel(Enum):
    """
    Types of processing level
    """
    # pylint: disable=invalid-name
    L2 = 'L2'
    L1C = 'L1C'
    L2A = 'L2A'
    L3B = 'L3B'
    L1TP = 'L1TP'
    GRD = 'GRD'
    MCD43A4 = 'MCD43A4'


class _Acquisition(Enum):
    """
    Types of satellite acquisition
    """
    # pylint: disable=invalid-name
    IW = 'IW'
    EW = 'EW'
    OLCI = 'OLCI'


class _Polarisation(Enum):
    """
    Types of Sentinel-1 polarisation
    """
    # pylint: disable=invalid-name
    DV = 'VV+VH'
    DH = 'HH+HV'
    SV = 'VV'
    SH = 'HH'


class _Resolution(Enum):
    """
    Types of Sentinel-1 resolution
    """
    MEDIUM = 'medium'
    HIGH = 'high'


class _OrbitDirection(Enum):
    """
    Types of Sentinel-1 orbit direction
    """
    ASCENDING = 'ascending'
    DESCENDING = 'descending'
    BOTH = 'both'


class DataSourceMeta(EnumMeta):
    """
    Meta class used to add custom (BYOC) datasources to DataSource enumerator.

    BYOC stands for Bring Your Own Data. For more details see: https://docs.sentinel-hub.com/api/latest/#/API/byoc
    """
    def __call__(cls, collection_id, *args, **kwargs):
        """
        This is executed whenever DataSource('something') is called.

        The method raises a ValueError if the 'something' does not match the format expected for collection id.
        """
        if not isinstance(collection_id, str):
            return super().__call__(collection_id, *args, **kwargs)

        collection_id_pattern = '.{8}-.{4}-.{4}-.{4}-.{12}'
        if not re.compile(collection_id_pattern).match(collection_id):
            raise ValueError("Given collection id does not match the expected format {}".format(collection_id_pattern))

        datasource_name = cls._custom_datasource_name(collection_id)
        if datasource_name not in cls._member_names_:
            extend_enum(cls, datasource_name, collection_id)

        return super().__call__(collection_id, *args, **kwargs)

    @staticmethod
    def _custom_datasource_name(collection_id):
        """
        Prepares a name for custom (BYOC) datasource, which is then added to DataSource enum.

        :param: collection_id: Collection id of the BYOC, user's input.
        :type: string
        :return: Name for custom (BYOC) datasource.
        :rtype: string
        """
        return 'BYOC_{}'.format(collection_id)


class DataSource(Enum, metaclass=DataSourceMeta):
    """ Enum constant class for types of satellite data

    Supported types are SENTINEL2_L1C, SENTINEL2_L2A, LANDSAT8, SENTINEL1_IW, SENTINEL1_EW, SENTINEL1_EW_SH,
    SENTINEL1_IW_ASC, SENTINEL1_EW_ASC, SENTINEL1_EW_SH_ASC, SENTINEL1_IW_DES, SENTINEL1_EW_DES, SENTINEL1_EW_SH_DES,
    DEM, MODIS, LANDSAT5, LANDSAT7, SENTINEL3, SENTINEL5P, ENVISAT_MERIS, SENTINEL2_L3B, LANDSAT8_L2A
    """
    SENTINEL2_L1C = (_Source.SENTINEL2, _ProcessingLevel.L1C)
    SENTINEL2_L2A = (_Source.SENTINEL2, _ProcessingLevel.L2A)
    SENTINEL1_IW = (_Source.SENTINEL1, _ProcessingLevel.GRD, _Acquisition.IW, _Polarisation.DV, _Resolution.HIGH,
                    _OrbitDirection.BOTH)
    SENTINEL1_EW = (_Source.SENTINEL1, _ProcessingLevel.GRD, _Acquisition.EW, _Polarisation.DH, _Resolution.MEDIUM,
                    _OrbitDirection.BOTH)
    SENTINEL1_EW_SH = (_Source.SENTINEL1, _ProcessingLevel.GRD, _Acquisition.EW, _Polarisation.SH, _Resolution.MEDIUM,
                       _OrbitDirection.BOTH)
    SENTINEL1_IW_ASC = (_Source.SENTINEL1, _ProcessingLevel.GRD, _Acquisition.IW, _Polarisation.DV, _Resolution.HIGH,
                        _OrbitDirection.ASCENDING)
    SENTINEL1_EW_ASC = (_Source.SENTINEL1, _ProcessingLevel.GRD, _Acquisition.EW, _Polarisation.DH, _Resolution.MEDIUM,
                        _OrbitDirection.ASCENDING)
    SENTINEL1_EW_SH_ASC = (_Source.SENTINEL1, _ProcessingLevel.GRD, _Acquisition.EW, _Polarisation.SH,
                           _Resolution.MEDIUM, _OrbitDirection.ASCENDING)
    SENTINEL1_IW_DES = (_Source.SENTINEL1, _ProcessingLevel.GRD, _Acquisition.IW, _Polarisation.DV, _Resolution.HIGH,
                        _OrbitDirection.DESCENDING)
    SENTINEL1_EW_DES = (_Source.SENTINEL1, _ProcessingLevel.GRD, _Acquisition.EW, _Polarisation.DH, _Resolution.MEDIUM,
                        _OrbitDirection.DESCENDING)
    SENTINEL1_EW_SH_DES = (_Source.SENTINEL1, _ProcessingLevel.GRD, _Acquisition.EW, _Polarisation.SH,
                           _Resolution.MEDIUM, _OrbitDirection.DESCENDING)
    DEM = (_Source.DEM, )
    MODIS = (_Source.MODIS, _ProcessingLevel.MCD43A4)
    LANDSAT8 = (_Source.LANDSAT8, _ProcessingLevel.L1TP)
    #: custom BYOC enum members are defined in DataSourceMeta
    # eocloud sources:
    LANDSAT5 = (_Source.LANDSAT5, _ProcessingLevel.GRD)
    LANDSAT7 = (_Source.LANDSAT7, _ProcessingLevel.GRD)
    SENTINEL3 = (_Source.SENTINEL3, _ProcessingLevel.L2, _Acquisition.OLCI)
    SENTINEL5P = (_Source.SENTINEL5P, _ProcessingLevel.L2)
    ENVISAT_MERIS = (_Source.ENVISAT_MERIS, )
    SENTINEL2_L3B = (_Source.SENTINEL2, _ProcessingLevel.L3B)
    LANDSAT8_L2A = (_Source.LANDSAT8, _ProcessingLevel.L2A)
    LANDSAT8_L1C = (_Source.LANDSAT8, _ProcessingLevel.L1C)

    @classmethod
    def get_wfs_typename(cls, data_source):
        """ Maps data source to string identifier for WFS

        :param data_source: One of the supported data sources
        :type data_source: DataSource
        :return: Product identifier for WFS
        :rtype: str
        """
        is_eocloud = SHConfig().has_eocloud_url()

        if data_source.is_custom():
            return 'DSS10-{}'.format(data_source.value)

        return {
            cls.SENTINEL2_L1C: 'S2.TILE',
            cls.SENTINEL2_L2A: 'SEN4CAP_S2L2A.TILE' if is_eocloud else 'DSS2',
            cls.SENTINEL1_IW: 'S1.TILE' if is_eocloud else 'DSS3',
            cls.SENTINEL1_EW: 'S1_EW.TILE' if is_eocloud else 'DSS3',
            cls.SENTINEL1_EW_SH: 'S1_EW_SH.TILE' if is_eocloud else 'DSS3',
            cls.SENTINEL1_IW_ASC: 'S1.TILE' if is_eocloud else 'DSS3',
            cls.SENTINEL1_EW_ASC: 'S1_EW.TILE' if is_eocloud else 'DSS3',
            cls.SENTINEL1_EW_SH_ASC: 'S1_EW_SH.TILE' if is_eocloud else 'DSS3',
            cls.SENTINEL1_IW_DES: 'S1.TILE' if is_eocloud else 'DSS3',
            cls.SENTINEL1_EW_DES: 'S1_EW.TILE' if is_eocloud else 'DSS3',
            cls.SENTINEL1_EW_SH_DES: 'S1_EW_SH.TILE' if is_eocloud else 'DSS3',
            cls.DEM: 'DSS4',
            cls.MODIS: 'DSS5',
            cls.LANDSAT8: 'L8.TILE' if is_eocloud else 'DSS6',
            # eocloud sources only:
            cls.LANDSAT5: 'L5.TILE',
            cls.LANDSAT7: 'L7.TILE',
            cls.SENTINEL3: 'S3.TILE',
            cls.SENTINEL5P: 'S5p_L2.TILE',
            cls.ENVISAT_MERIS: 'ENV.TILE',
            cls.SENTINEL2_L3B: 'SEN4CAP_S2L3B.TILE',
            cls.LANDSAT8_L2A: 'SEN4CAP_L8L2A.TILE'
        }[data_source]

    def api_identifier(self):
        """ Returns Sentinel Hub API identifier string

        :return: A data source identifier string
        :rtype: str
        """
        return {
            DataSource.SENTINEL2_L1C: 'S2L1C',
            DataSource.SENTINEL2_L2A: 'S2L2A',
            DataSource.LANDSAT8_L1C: 'L8L1C',
            DataSource.DEM: 'DEM',
            DataSource.MODIS: 'MODIS'
        }[self]

    def bands(self):
        """ Get available bands for a particular data source
        """
        return {
            DataSource.SENTINEL2_L1C: [
                "B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B10", "B11", "B12"
            ],
            DataSource.SENTINEL2_L2A: [
                "B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11", "B12"
            ],
            DataSource.LANDSAT8_L1C: [
                "B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09", "B10", "B11"
            ],
            DataSource.DEM: [
                "DEM"
            ],
            DataSource.MODIS: [
                "B01", "B02", "B03", "B04", "B05", "B06", "B07"
            ]
        }[self]

    def is_sentinel1(self):
        """Checks if source is Sentinel-1

        Example: ``DataSource.SENTINEL1_IW.is_sentinel1()`` or ``DataSource.is_sentinel1(DataSource.SENTINEL1_IW)``

        :param self: One of the supported data sources
        :type self: DataSource
        :return: `True` if source is Sentinel-1 and `False` otherwise
        :rtype: bool
        """
        return self.value[0] is _Source.SENTINEL1

    def contains_orbit_direction(self, orbit_direction):
        """Checks if Sentine-1 data source contains given orbit direction.
        Note: Data sources with "both" orbit directions contain ascending and descending orbit directions.

        :param self: One of the supported Sentinel-1 data sources
        :type self: DataSource
        :param orbit_direction: One of the orbit directions
        :type orbit_direction: string
        :return: `True` if data source contains the orbit direction
        :return: bool
        """
        if not self.is_sentinel1():
            raise ValueError("Orbit direction can only be checked for Sentinel-1 data source.")
        return self.value[5].name.upper() in [orbit_direction.upper(), _OrbitDirection.BOTH.value.upper()]

    def is_timeless(self):
        """Checks if data source is time independent

        Example: ``DataSource.DEM.is_timeless()`` or ``DataSource.is_timeless(DataSource.DEM)``

        :param self: One of the supported data sources
        :type self: DataSource
        :return: `True` if data source is time independent and `False` otherwise
        :rtype: bool
        """
        return self.value[0] is _Source.DEM

    def is_uswest_source(self):
        """Checks if data source via Sentinel Hub services is available at US West server

        Example: ``DataSource.LANDSAT8.is_uswest_source()`` or ``DataSource.is_uswest_source(DataSource.LANDSAT8)``

        :param self: One of the supported data sources
        :type self: DataSource
        :return: `True` if data source exists at US West server and `False` otherwise
        :rtype: bool
        """
        return not SHConfig().has_eocloud_url() and self.value[0] in [_Source.LANDSAT8, _Source.MODIS, _Source.DEM]

    @classmethod
    def get_available_sources(cls):
        """ Returns which data sources are available for configured Sentinel Hub OGC URL

        :return: List of available data sources
        :rtype: list(sentinelhub.DataSource)
        """
        if SHConfig().has_eocloud_url():
            return [cls.SENTINEL2_L1C, cls.SENTINEL2_L2A, cls.SENTINEL2_L3B, cls.SENTINEL1_IW, cls.SENTINEL1_EW,
                    cls.SENTINEL1_EW_SH, cls.SENTINEL3, cls.SENTINEL5P, cls.LANDSAT5, cls.LANDSAT7, cls.LANDSAT8,
                    cls.LANDSAT8_L2A, cls.ENVISAT_MERIS]

        return [cls.SENTINEL2_L1C, cls.SENTINEL2_L2A, cls.SENTINEL1_IW, cls.SENTINEL1_EW, cls.SENTINEL1_EW_SH,
                cls.SENTINEL1_IW_ASC, cls.SENTINEL1_EW_ASC, cls.SENTINEL1_EW_SH_ASC, cls.SENTINEL1_IW_DES,
                cls.SENTINEL1_EW_DES, cls.SENTINEL1_EW_SH_DES, cls.DEM, cls.MODIS, cls.LANDSAT8,
                *cls.get_custom_sources()]

    @classmethod
    def get_custom_sources(cls):
        """Returns the list of all custom (BYOC) datasources, which have ben added to the DataSource enumerator.

        :return: List of custom (BYOC) datasources
        :rtype: list(sentinelhub.DataSource)
        """
        return [datasource for datasource in cls if datasource.is_custom()]

    def is_custom(self):
        """ Checks is if datasource is a custom Sentinel Hub BYOC data source

        :return: True if datasource is custom and False otherwise
        :rtype: bool
        """
        return self.name.startswith('BYOC_')


class CRSMeta(EnumMeta):
    """ Metaclass used for building CRS Enum class
    """
    def __new__(mcs, cls, bases, classdict):
        """ This is executed at the beginning of runtime when CRS class is created
        """
        for direction, direction_value in [('N', '6'), ('S', '7')]:
            for zone in range(1, 61):
                classdict['UTM_{}{}'.format(zone, direction)] = '32{}{}'.format(direction_value, str(zone).zfill(2))

        return super().__new__(mcs, cls, bases, classdict)

    def __call__(cls, value, *args, **kwargs):
        """ This is executed whenever CRS('something') is called
        """
        return super().__call__(CRSMeta._parse_crs(value), *args, **kwargs)

    @staticmethod
    def _parse_crs(value):
        """ Method for parsing different inputs representing the same CRS enum. Example:
        """
        if isinstance(value, int):
            return str(value)
        if isinstance(value, str):
            return value.strip('epsgEPSG: ')
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

    @classmethod
    def has_value(cls, value):
        """ Tests whether CRS contains a constant defined with string `value`.

        :param value: The string representation of the enum constant.
        :type value: str
        :return: `True` if there exists a constant with string value `value`, `False` otherwise
        :rtype: bool
        """
        return any(value == item.value for item in cls)

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
        return 'EPSG:{}'.format(CRS(self).value)

    @property
    def opengis_string(self):
        """ Returns an URL to OGC webpage where the CRS is defined

        :return: An URL with CRS definition
        :rtype: str
        """
        return 'http://www.opengis.net/def/crs/EPSG/0/{}'.format(self.epsg)

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

        :param self: An enum constant representing a coordinate reference system.
        :type self: CRS
        :return: pyproj projection class
        :rtype: pyproj.Proj
        """
        return pyproj.Proj(init=self.ogc_string(), preserve_units=True)

    @functools.lru_cache(maxsize=10)
    def get_transform_function(self, other):
        """ Returns a function for transforming geometrical objects from one CRS to another. The function will support
        transformations between any objects that pyproj supports.
        For better time performance this method will cache results of 10 most recently used pairs of CRS classes.

        :param self: Initial CRS
        :type self: CRS
        :param other: Target CRS
        :type other: CRS
        :return: A projection function obtained from pyproj package
        :rtype: function
        """
        if pyproj.__version__ >= '2':
            return pyproj.Transformer.from_proj(self.projection(), other.projection(), skip_equivalent=True).transform

        return functools.partial(pyproj.transform, self.projection(), other.projection())

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


class CustomUrlParam(Enum):
    """ Enum class to represent supported custom url parameters of OGC services

    Supported parameters are `SHOWLOGO`, `ATMFILTER`, `EVALSCRIPT`, `EVALSCRIPTURL`, `PREVIEW`, `QUALITY`, `UPSAMPLING`,
    `DOWNSAMPLING`, `TRANSPARENT`, `BGCOLOR` and `GEOMETRY`.

    See http://sentinel-hub.com/develop/documentation/api/custom-url-parameters and
    https://www.sentinel-hub.com/develop/documentation/api/ogc_api/wms-parameters for more information.
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
    BGCOLOR = 'BgColor'
    GEOMETRY = 'Geometry'

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
    """ Enum class to represent supported image file formats

    Supported file formats are TIFF 8-bit, TIFF 16-bit, TIFF 32-bit float, PNG, JPEG, JPEG2000, JSON, CSV, ZIP, HDF5,
    XML, GML, RAW
    """
    TIFF = 'tiff'
    TIFF_d8 = 'tiff;depth=8'
    TIFF_d16 = 'tiff;depth=16'  # This is the same as TIFF
    TIFF_d32f = 'tiff;depth=32f'
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

    def is_image_format(self):
        """ Checks whether file format is an image format

        Example: ``MimeType.PNG.is_image_format()`` or ``MimeType.is_image_format(MimeType.PNG)``

        :param self: File format
        :type self: MimeType
        :return: `True` if file is in image format, `False` otherwise
        :rtype: bool
        """
        return self in frozenset([MimeType.TIFF, MimeType.TIFF_d8, MimeType.TIFF_d16, MimeType.TIFF_d32f, MimeType.PNG,
                                  MimeType.JP2, MimeType.JPG])

    def is_api_format(self):
        """ Checks if mime type is supported by Sentinel Hub API

        :return: True if API supports this format and False otherwise
        :rtype: bool
        """
        return self in frozenset([MimeType.JPG, MimeType.PNG, MimeType.TIFF, MimeType.JSON])

    def is_tiff_format(self):
        """ Checks whether file format is a TIFF image format

        Example: ``MimeType.TIFF.is_tiff_format()`` or ``MimeType.is_tiff_format(MimeType.TIFF)``

        :param self: File format
        :type self: MimeType
        :return: `True` if file is in image format, `False` otherwise
        :rtype: bool
        """
        return self in frozenset([MimeType.TIFF, MimeType.TIFF_d8, MimeType.TIFF_d16, MimeType.TIFF_d32f])

    @classmethod
    def has_value(cls, value):
        """ Tests whether MimeType contains a constant defined with string ``value``

        :param value: The string representation of the enum constant
        :type value: str
        :return: `True` if there exists a constant with string value ``value``, `False` otherwise
        :rtype: bool
        """
        return any(value == item.value for item in cls)

    def get_string(self):
        """ Get file format as string

        :return: String describing the file format
        :rtype: str
        """
        if self is MimeType.TAR:
            return 'application/x-tar'
        if self is MimeType.JSON:
            return 'application/json'
        if self in [MimeType.TIFF_d8, MimeType.TIFF_d16, MimeType.TIFF_d32f]:
            return 'image/{}'.format(self.value)
        if self is MimeType.JP2:
            return 'image/jpeg2000'
        if self is MimeType.RAW:
            return self.value
        return mimetypes.types_map['.' + self.value]

    def get_sample_type(self):
        """ Returns sampleType used in Sentinel-Hub evalscripts.

        :return: sampleType
        :rtype: str
        :raises: ValueError
        """
        try:
            return {
                MimeType.TIFF: 'INT16',
                MimeType.TIFF_d8: 'INT8',
                MimeType.TIFF_d16: 'INT16',
                MimeType.TIFF_d32f: 'FLOAT32'
            }[self]
        except IndexError:
            raise ValueError('Type {} is not supported by this method'.format(self))

    def get_expected_max_value(self):
        """ Returns max value of image `MimeType` format and raises an error if it is not an image format

        Note: For `MimeType.TIFF_d32f` it will return ``1.0`` as that is expected maximum for an image even though it
        could be higher.

        :return: A maximum value of specified image format
        :rtype: int or float
        :raises: ValueError
        """
        try:
            return {
                MimeType.TIFF: 65535,
                MimeType.TIFF_d8: 255,
                MimeType.TIFF_d16: 65535,
                MimeType.TIFF_d32f: 1.0,
                MimeType.PNG: 255,
                MimeType.JPG: 255,
                MimeType.JP2: 10000
            }[self]
        except IndexError:
            raise ValueError('Type {} is not supported by this method'.format(self))

    @staticmethod
    def from_string(mime_type_str):
        """ Parses mime type from a file extension string

        :param mime_type_str: A file extension string
        :type mime_type_str: str
        :return: A mime type enum
        :rtype: MimeType
        """
        if mime_type_str == 'jpeg':
            return MimeType.JPG

        return MimeType(mime_type_str)


class RequestType(Enum):
    """ Enum constant class for GET/POST request type """
    GET = 'GET'
    POST = 'POST'


class SHConstants:
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
                       ['qi/{}'.format(qi_report) for qi_report in QUALITY_REPORTS] +\
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
                  **{'qi/{}'.format(qi_report): MimeType.XML for qi_report in QUALITY_REPORTS},
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

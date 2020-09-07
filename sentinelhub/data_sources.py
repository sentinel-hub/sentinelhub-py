"""
Module defining data sources
"""
import re
import warnings
from enum import Enum, EnumMeta
from typing import Tuple
from dataclasses import dataclass, asdict

from aenum import extend_enum

from .config import SHConfig
from .constants import ServiceUrl
from .exceptions import SHDeprecationWarning


class _Source:
    """ Types of satellite sources
    """
    SENTINEL2 = 'Sentinel-2'
    SENTINEL1 = 'Sentinel-1'
    LANDSAT8 = 'Landsat 8'
    MODIS = 'MODIS'
    DEM = 'Mapzen DEM'
    BYOC = 'BYOC'
    BATCH = 'BATCH'
    LANDSAT5 = 'Landsat 5'
    LANDSAT7 = 'Landsat 7'
    SENTINEL3 = 'Sentinel-3'
    SENTINEL5P = 'Sentinel-5P'
    ENVISAT_MERIS = 'Envisat Meris'


class _SensorType:
    """ Types of satellite sensors
    """
    # pylint: disable=invalid-name
    MSI = 'MSI'
    IW = 'IW'
    EW = 'EW'
    OLCI = 'OLCI'
    SLSTR = 'SLSTR'
    TROPOMI = 'TROPOMI'


class _ProcessingLevel:
    """ Types of processing level
    """
    # pylint: disable=invalid-name
    L1B = 'L1B'
    L1C = 'L1C'
    L2A = 'L2A'
    L3B = 'L3B'
    L2 = 'L2'
    GRD = 'GRD'
    MCD43A4 = 'MCD43A4'


class _Polarization:
    """ Types of SAR polarizations
    """
    # pylint: disable=invalid-name
    DV = 'DV'
    DH = 'DH'
    SV = 'SV'
    SH = 'SH'


class _Resolution:
    """ Types of product resolution (specific to Sentinel-1 sources)
    """
    MEDIUM = 'MEDIUM'
    HIGH = 'HIGH'


class OrbitDirection:
    """ Types of orbit directions
    """
    ASCENDING = 'ASCENDING'
    DESCENDING = 'DESCENDING'
    BOTH = 'BOTH'


class _Bands:
    """ Different collections of bands
    """
    SENTINEL2_L1C = ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12')
    SENTINEL2_L2A = ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B11', 'B12')
    SENTINEL3_OLCI = tuple(f'B{str(index).zfill(2)}' for index in range(1, 22))
    SENTINEL3_SLSTR = ('S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'S9', 'F1', 'F2')
    LANDSAT8 = ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10', 'B11')
    DEM = ('DEM',)
    MODIS = ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07')
    SENTINEL1_IW = ('VV', 'VH')
    SENTINEL1_EW = ('HH', 'HV')
    SENTINEL1_EW_SH = ('HH',)
    SENTINEL5P = ('AER_AI_340_380', 'AER_AI_354_388', 'CLOUD_BASE_HEIGHT', 'CLOUD_BASE_PRESSURE', 'CLOUD_FRACTION',
                  'CLOUD_OPTICAL_THICKNESS', 'CLOUD_TOP_HEIGHT', 'CLOUD_TOP_PRESSURE', 'CO', 'HCHO', 'NO2', 'O3',
                  'SO2', 'CH4')


class _DataSourceMeta(EnumMeta):
    """ Meta class that builds DataSource class enums
    """
    def __call__(cls, value, *args, **kwargs):
        """ This is executed whenever DataSource('something') is called

        This implements a shortcut to create a BYOC data source by calling DataSource('<BYOC collection ID>')
        """
        # pylint: disable=signature-differs
        if not isinstance(value, str):
            return super().__call__(value, *args, **kwargs)

        byoc_data_source = cls.define_byoc(value)
        message = 'This way of defining BYOC data source is deprecated and will soon be removed. Please use ' \
                  'DataSource.from_byoc(collection_id) instead'
        warnings.warn(message, category=SHDeprecationWarning)

        return byoc_data_source


@dataclass(frozen=True)
class DataSourceDefinition:
    """ An immutable definition of a data source

    Check `DataSource.define` for more info about attributes of this class
    """
    api_id: str = None
    wfs_id: str = None
    service_url: str = None
    source: str = None
    sensor_type: str = None
    processing_level: str = None
    polarization: str = None
    resolution: str = None
    orbit_direction: str = None
    bands: Tuple[str, ...] = None
    collection_id: str = None
    is_timeless: bool = False

    def __post_init__(self):
        """ In case a list of bands has been given this makes sure to cast it into a tuple
        """
        if isinstance(self.bands, list):
            object.__setattr__(self, 'bands', tuple(self.bands))

    def __repr__(self):
        """ A nicer representation of parameters that define a data source
        """
        valid_params = {name: value for name, value in asdict(self).items() if value is not None}
        params_repr = '\n  '.join(f'{name}: {value}' for name, value in valid_params.items())
        return f'{self.__class__.__name__}(\n  {params_repr}\n)'

    def derive(self, **params):
        """ Create a new data source definition from current definition and parameters that override current parameters

        :param params: Any of DataSourceDefinition attributes
        :return: A new data source definition
        :rtype: DataSourceDefinition
        """
        derived_params = asdict(self)
        for name, value in params.items():
            derived_params[name] = value

        return DataSourceDefinition(**derived_params)


class DataSource(Enum, metaclass=_DataSourceMeta):
    """ An enum class for data sources

    It contains a collection of predefined data sources, which are the most commonly used with Sentinel Hub service.
    Additionally it also allows defining new data sources by specifying data source parameters relevant for
    the service. Check `DataSource.define` and similar methods for more.
    """
    SENTINEL2_L1C = DataSourceDefinition(
        api_id='S2L1C',
        wfs_id='DSS1',
        source=_Source.SENTINEL2,
        sensor_type=_SensorType.MSI,
        processing_level=_ProcessingLevel.L1C,
        bands=_Bands.SENTINEL2_L1C
    )
    SENTINEL2_L2A = DataSourceDefinition(
        api_id='S2L2A',
        wfs_id='DSS2',
        source=_Source.SENTINEL2,
        sensor_type=_SensorType.MSI,
        processing_level=_ProcessingLevel.L2A,
        bands=_Bands.SENTINEL2_L2A
    )

    SENTINEL1 = DataSourceDefinition(
        api_id='S1GRD',
        wfs_id='DSS3',
        source=_Source.SENTINEL1,
        processing_level=_ProcessingLevel.GRD,
        orbit_direction=OrbitDirection.BOTH
    )
    SENTINEL1_IW = SENTINEL1.derive(
        sensor_type=_SensorType.IW,
        polarization=_Polarization.DV,
        resolution=_Resolution.HIGH,
        bands=_Bands.SENTINEL1_IW
    )
    SENTINEL1_IW_ASC = SENTINEL1_IW.derive(
        orbit_direction=OrbitDirection.ASCENDING
    )
    SENTINEL1_IW_DES = SENTINEL1_IW.derive(
        orbit_direction=OrbitDirection.DESCENDING
    )
    SENTINEL1_EW = SENTINEL1.derive(
        sensor_type=_SensorType.EW,
        polarization=_Polarization.DH,
        resolution=_Resolution.MEDIUM,
        bands=_Bands.SENTINEL1_EW
    )
    SENTINEL1_EW_ASC = SENTINEL1_EW.derive(
        orbit_direction=OrbitDirection.ASCENDING
    )
    SENTINEL1_EW_DES = SENTINEL1_EW.derive(
        orbit_direction=OrbitDirection.DESCENDING
    )
    SENTINEL1_EW_SH = SENTINEL1_EW.derive(
        polarization=_Polarization.SH,
        bands=_Bands.SENTINEL1_EW_SH
    )
    SENTINEL1_EW_SH_ASC = SENTINEL1_EW_SH.derive(
        orbit_direction=OrbitDirection.ASCENDING
    )
    SENTINEL1_EW_SH_DES = SENTINEL1_EW_SH.derive(
        orbit_direction=OrbitDirection.DESCENDING
    )

    DEM = DataSourceDefinition(
        api_id='DEM',
        wfs_id='DSS4',
        source=_Source.DEM,
        bands=_Bands.DEM,
        is_timeless=True
    )
    MODIS = DataSourceDefinition(
        api_id='MODIS',
        wfs_id='DSS5',
        service_url=ServiceUrl.USWEST,
        source=_Source.MODIS,
        processing_level=_ProcessingLevel.MCD43A4,
        bands=_Bands.MODIS
    )
    LANDSAT8 = DataSourceDefinition(
        api_id='L8L1C',
        wfs_id='DSS6',
        service_url=ServiceUrl.USWEST,
        source=_Source.LANDSAT8,
        processing_level=_ProcessingLevel.L1C,
        bands=_Bands.LANDSAT8
    )

    SENTINEL5P = DataSourceDefinition(
        api_id='S5PL2',
        wfs_id='DSS7',
        service_url=ServiceUrl.CREODIAS,
        source=_Source.SENTINEL5P,
        sensor_type=_SensorType.TROPOMI,
        processing_level=_ProcessingLevel.L2,
        bands=_Bands.SENTINEL5P
    )
    SENTINEL3_OLCI = DataSourceDefinition(
        api_id='S3OLCI',
        wfs_id='DSS8',
        service_url=ServiceUrl.CREODIAS,
        source=_Source.SENTINEL3,
        sensor_type=_SensorType.OLCI,
        processing_level=_ProcessingLevel.L1B,
        bands=_Bands.SENTINEL3_OLCI
    )
    SENTINEL3_SLSTR = DataSourceDefinition(
        api_id='S3SLSTR',
        wfs_id='DSS9',
        service_url=ServiceUrl.CREODIAS,
        source=_Source.SENTINEL3,
        sensor_type=_SensorType.SLSTR,
        processing_level=_ProcessingLevel.L1B,
        bands=_Bands.SENTINEL3_SLSTR
    )

    # EOCloud sources (which are only available on a development eocloud service):
    LANDSAT5 = DataSourceDefinition(
        wfs_id='L5.TILE',
        service_url=ServiceUrl.EOCLOUD,
        processing_level=_ProcessingLevel.GRD
    )
    LANDSAT7 = DataSourceDefinition(
        wfs_id='L7.TILE',
        service_url=ServiceUrl.EOCLOUD,
        processing_level=_ProcessingLevel.GRD
    )
    ENVISAT_MERIS = DataSourceDefinition(
        wfs_id='ENV.TILE',
        service_url=ServiceUrl.EOCLOUD,
        source=_Source.ENVISAT_MERIS
    )

    @classmethod
    def define(cls, name, *, api_id=None, wfs_id=None, service_url=None, source=None, sensor_type=None,
               processing_level=None, polarization=None, resolution=None, orbit_direction=None, bands=None,
               collection_id=None, is_timeless=False):
        """ Define a new data source

        Note that all parameters, except `name` are optional. If a data source definition won't be used for a certain
        use case (e.g. Processing API, WFS, etc.), parameters for that use case don't have to be defined

        :param name: A name of a new data source
        :type name: str
        :param api_id: An ID to be used for Sentinel Hub Processing API
        :type api_id: str or None
        :param wfs_id: An ID to be used for Sentinel Hub WFS service
        :type wfs_id: str or None
        :param service_url: A base URL of Sentinel Hub service deployment from where to download data. If it is not
            specified, a `sh_base_url` from a config will be used by default
        :type service_url: str or None
        :param source: A name of a satellite source
        :type source: str or None
        :param sensor_type: A type of a satellite's sensor
        :type sensor_type: str or None
        :param processing_level: A level of processing applied on satellite data
        :type processing_level: str or None
        :param polarization: A type of polarization
        :type polarization: str or None
        :param resolution: A type of (Sentinel-1) resolution
        :type resolution: str or None
        :param orbit_direction: A direction of satellite's orbit by which to filter satellite's data
        :type orbit_direction: str or None
        :param bands: Names of data source bands
        :type bands: tuple(str) or None
        :param collection_id: An ID of a BYOC or BATCH collection
        :type collection_id: str or None
        :param is_timeless: `True` if a data source can be filtered by time dimension and `False` otherwise
        :type is_timeless: bool
        :return: A new data source
        :rtype: DataSource
        """
        definition = DataSourceDefinition(
            api_id=api_id,
            wfs_id=wfs_id,
            service_url=service_url,
            source=source,
            sensor_type=sensor_type,
            processing_level=processing_level,
            polarization=polarization,
            resolution=resolution,
            orbit_direction=orbit_direction,
            bands=bands,
            collection_id=collection_id,
            is_timeless=is_timeless
        )
        cls._try_add_data_source(name, definition)
        return cls(definition)

    def define_from(self, name, **params):
        """ Define a new data source from an existing one

        :param name: A name of a new data source
        :type name: str
        :param params: Any parameter to override current data source parameters
        :return: A new data source
        :rtype: DataSource
        """
        definition = self.value
        new_definition = definition.derive(**params)

        self._try_add_data_source(name, new_definition)
        return DataSource(new_definition)

    @classmethod
    def _try_add_data_source(cls, name, definition):
        """ Tries adding a new data source definition. If the exact enum has already been defined then it won't do
        anything. However, if either a name or a definition has already been matched with another name or definition
        then it will raise an error.
        """
        is_name_defined = name in cls.__members__
        is_enum_defined = is_name_defined and cls.__members__[name].value == definition
        is_definition_defined = definition in cls._value2member_map_

        if is_enum_defined:
            return

        if not is_name_defined and not is_definition_defined:
            extend_enum(cls, name, definition)
            return

        if is_name_defined:
            raise ValueError(f"Data source name '{name}' is already taken by another data source")
        raise ValueError(f'Data source definition is already taken by a data source with a different name')

    @classmethod
    def define_byoc(cls, collection_id, **params):
        """ Defines a BYOC data source

        :param collection_id: An ID of a data collection
        :type collection_id: str
        :param params: Any parameter to override default BYOC data source parameters
        :return: A new data source
        :rtype: DataSource
        """
        params['name'] = params.get('name', f'BYOC_{collection_id}')
        params['api_id'] = params.get('api_id', f'byoc-{collection_id}')
        params['wfs_id'] = params.get('wfs_id', f'DSS10-{collection_id}')
        params['source'] = params.get('source', _Source.BYOC)
        params['collection_id'] = collection_id
        return cls.define(**params)

    @classmethod
    def define_batch(cls, collection_id, **params):
        """ Defines a BATCH data source

        :param collection_id: An ID of a data collection
        :type collection_id: str
        :param params: Any parameter to override default BATCH data source parameters
        :return: A new data source
        :rtype: DataSource
        """
        params['name'] = params.get('name', f'BATCH_{collection_id}')
        params['api_id'] = params.get('api_id', f'batch-{collection_id}')
        # WFS is at the moment not yet supported
        params['source'] = params.get('source', _Source.BATCH)
        params['collection_id'] = collection_id
        return cls.define(**params)

    @property
    def api_id(self):
        """ Provides a Sentinel Hub Processing API identifier or raises an error if it is not defined

        :return: An identifier
        :rtype: str
        :raises: ValueError
        """
        if self.value.api_id is None:
            raise ValueError(f'Data source {self.name} is missing a Sentinel Hub Processing API identifier')
        return self.value.api_id

    @property
    def wfs_id(self):
        """ Provides a Sentinel Hub WFS identifier or raises an error if it is not defined

        :return: An identifier
        :rtype: str
        :raises: ValueError
        """
        if self.value.wfs_id is None:
            raise ValueError(f'Data source {self.name} is missing a Sentinel Hub WFS identifier')
        return self.value.wfs_id

    @property
    def bands(self):
        """ Provides band names available for the data source

        :return: A tuple of band names
        :rtype: tuple(str)
        :raises: ValueError
        """
        if self.value.bands is None:
            raise ValueError(f'Data source {self.name} does not define bands')
        return self.value.bands

    def __getattr__(self, item, *args, **kwargs):
        """ The following insures that any attribute from DataSourceDefinition, which is already not a
        property or an attribute of DataSource, becomes an attribute of DataSource
        """
        if not item.startswith('_') and hasattr(self, 'value') and isinstance(self.value, DataSourceDefinition):
            definition_dict = asdict(self.value)
            if item in definition_dict:
                return definition_dict[item]

        return super().__getattr__(item, *args, **kwargs)

    @property
    def is_sentinel1(self):
        """ Checks if data source is Sentinel-1

        Example: ``DataSource.SENTINEL1_IW.is_sentinel1``

        :return: `True` if source is Sentinel-1 and `False` otherwise
        :rtype: bool
        """
        return self.source == _Source.SENTINEL1

    def contains_orbit_direction(self, orbit_direction):
        """ Checks if a data source contains given orbit direction

        :param orbit_direction: An orbit direction
        :type orbit_direction: string
        :return: `True` if data source contains the orbit direction
        :return: bool
        """
        defined_direction = self.orbit_direction
        if defined_direction is None or defined_direction.upper() == OrbitDirection.BOTH:
            return True
        return orbit_direction.upper() == defined_direction.upper()

    @classmethod
    def get_available_sources(cls, config=None):
        """ Returns which data sources are available for configured Sentinel Hub OGC URL

        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        :return: List of available data sources
        :rtype: list(DataSource)
        """
        config = config or SHConfig()
        is_eocloud = config.has_eocloud_url()

        return [data_source for data_source in cls
                if (data_source.service_url == ServiceUrl.EOCLOUD) == is_eocloud]


def _raise_invalid_id(collection_id):
    """ Checks if a given collection ID conforms to an expected pattern and raises an error if it doesn't
    """
    collection_id_pattern = '.{8}-.{4}-.{4}-.{4}-.{12}'

    if not re.compile(collection_id_pattern).match(collection_id):
        raise ValueError("Given collection id does not match the expected format {}".format(collection_id_pattern))

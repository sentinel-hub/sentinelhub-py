"""
Module defining data collections
"""
import re
import warnings
from enum import Enum, EnumMeta
from typing import Tuple
from dataclasses import dataclass, asdict, field

from aenum import extend_enum

from .config import SHConfig
from .constants import ServiceUrl
from .exceptions import SHDeprecationWarning


class _CollectionType:
    """ Types of Sentinel Hub data collections
    """
    SENTINEL1 = 'Sentinel-1'
    SENTINEL2 = 'Sentinel-2'
    SENTINEL3 = 'Sentinel-3'
    SENTINEL5P = 'Sentinel-5P'
    LANDSAT_MSS = 'Landsat 1-5 MSS'
    LANDSAT_TM = 'Landsat 4-5 TM'
    LANDSAT5 = 'Landsat 5'
    LANDSAT_ETM = 'Landsat 7 ETM+'
    LANDSAT_OT = 'Landsat 8 OLI and TIRS'
    MODIS = 'MODIS'
    ENVISAT_MERIS = 'Envisat Meris'
    DEM = 'DEM'
    BYOC = 'BYOC'
    BATCH = 'BATCH'


class _SensorType:
    """ Satellite sensors
    """
    # pylint: disable=invalid-name
    MSI = 'MSI'
    OLI_TIRS = 'OLI-TIRS'
    TM = 'TM'
    ETM = 'ETM+'
    MSS = 'MSS'
    C_SAR = 'C-SAR'
    OLCI = 'OLCI'
    SLSTR = 'SLSTR'
    TROPOMI = 'TROPOMI'


class _ProcessingLevel:
    """ Processing levels
    """
    # pylint: disable=invalid-name
    L1 = 'L1'
    L2 = 'L2'
    L1B = 'L1B'
    L1C = 'L1C'
    L2A = 'L2A'
    L3B = 'L3B'
    GRD = 'GRD'


class _SwathMode:
    """ Swath modes for SAR sensors
    """
    # pylint: disable=invalid-name
    IW = 'IW'
    EW = 'EW'
    SM = 'SM'
    WV = 'WV'


class _Polarization:
    """ SAR polarizations
    """
    # pylint: disable=invalid-name
    DV = 'DV'
    DH = 'DH'
    SV = 'SV'
    SH = 'SH'
    HH = 'HH'
    HV = 'HV'
    VV = 'VV'
    VH = 'VH'


class _Resolution:
    """ Product resolution (specific to Sentinel-1 collections)
    """
    MEDIUM = 'MEDIUM'
    HIGH = 'HIGH'


class OrbitDirection:
    """ Orbit directions
    """
    ASCENDING = 'ASCENDING'
    DESCENDING = 'DESCENDING'
    BOTH = 'BOTH'


class _Bands:
    """ Different collections of bands
    """
    SENTINEL1_IW = ('VV', 'VH')
    SENTINEL1_EW = ('HH', 'HV')
    SENTINEL1_EW_SH = ('HH',)
    SENTINEL2_L1C = ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12')
    SENTINEL2_L2A = ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B11', 'B12')
    SENTINEL3_OLCI = tuple(f'B{str(index).zfill(2)}' for index in range(1, 22))
    SENTINEL3_SLSTR = ('S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'S9', 'F1', 'F2')
    SENTINEL5P = ('AER_AI_340_380', 'AER_AI_354_388', 'CLOUD_BASE_HEIGHT', 'CLOUD_BASE_PRESSURE', 'CLOUD_FRACTION',
                  'CLOUD_OPTICAL_THICKNESS', 'CLOUD_TOP_HEIGHT', 'CLOUD_TOP_PRESSURE', 'CO', 'HCHO', 'NO2', 'O3',
                  'SO2', 'CH4')
    LANDSAT_MSS = ('B01', 'B02', 'B03', 'B04')
    LANDSAT_TM = ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07')
    LANDSAT_ETM_L1 = ('B01', 'B02', 'B03', 'B04', 'B05', 'B06_VCID1', 'B06_VCID2', 'B07', 'B08')
    LANDSAT_ETM_L2 = ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07')
    LANDSAT_OT = ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10', 'B11')
    LANDSAT_OT_L2 = ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B10')
    DEM = ('DEM',)
    MODIS = ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07')


class _DataCollectionMeta(EnumMeta):
    """ Meta class that builds DataCollection class enums
    """
    def __getattribute__(cls, item, *args, **kwargs):
        """ This is executed whenever `DataCollection.SOMETHING` is called

        Extended method handles cases where a collection has been renamed. It provides a new collection and raises a
        deprecation warning.
        """
        if item in _RENAMED_COLLECTIONS:
            old_item = item
            item = _RENAMED_COLLECTIONS[old_item]

            message = f'DataCollection.{old_item} had been renamed into DataCollection.{item}. Please switch to the ' \
                      'new name as the old one will soon be removed.'
            warnings.warn(message, category=SHDeprecationWarning)

        return super().__getattribute__(item, *args, **kwargs)

    def __call__(cls, value, *args, **kwargs):
        """ This is executed whenever `DataCollection('something')` is called

        This solves a problem of pickling a custom DataCollection and unpickling it in another process
        """
        if isinstance(value, DataCollectionDefinition) and value not in cls._value2member_map_ and value._name:
            cls._try_add_data_collection(value._name, value)

        return super().__call__(value, *args, **kwargs)


@dataclass(frozen=True)
class DataCollectionDefinition:
    """ An immutable definition of a data collection

    Check `DataCollection.define` for more info about attributes of this class
    """
    # pylint: disable=too-many-instance-attributes
    api_id: str = None
    catalog_id: str = None
    wfs_id: str = None
    service_url: str = None
    collection_type: str = None
    sensor_type: str = None
    processing_level: str = None
    swath_mode: str = None
    polarization: str = None
    resolution: str = None
    orbit_direction: str = None
    timeliness: str = None
    bands: Tuple[str, ...] = None
    collection_id: str = None
    is_timeless: bool = False
    has_cloud_coverage: bool = False
    dem_instance: str = None
    # The following parameter is used to preserve custom DataCollection name during pickling and unpickling process:
    _name: str = field(default=None, compare=False)

    def __post_init__(self):
        """ In case a list of bands has been given this makes sure to cast it into a tuple
        """
        if isinstance(self.bands, list):
            object.__setattr__(self, 'bands', tuple(self.bands))

    def __repr__(self):
        """ A nicer representation of parameters that define a data collection
        """
        valid_params = {name: value for name, value in asdict(self).items() if value is not None}
        params_repr = '\n  '.join(f'{name}: {value}' for name, value in valid_params.items() if name != '_name')
        return f'{self.__class__.__name__}(\n  {params_repr}\n)'

    def derive(self, **params):
        """ Create a new data collection definition from current definition and parameters that override current
        parameters

        :param params: Any of DataCollectionDefinition attributes
        :return: A new data collection definition
        :rtype: DataCollectionDefinition
        """
        derived_params = asdict(self)
        for name, value in params.items():
            derived_params[name] = value

        return DataCollectionDefinition(**derived_params)


_RENAMED_COLLECTIONS = {  # DataCollection renaming for backwards-compatibility
    'LANDSAT15_L1': 'LANDSAT_MSS_L1',
    'LANDSAT45_L1': 'LANDSAT_TM_L1',
    'LANDSAT45_L2': 'LANDSAT_TM_L2',
    'LANDSAT7_L1': 'LANDSAT_ETM_L1',
    'LANDSAT7_L2': 'LANDSAT_ETM_L2',
    'LANDSAT8': 'LANDSAT_OT_L1',
    'LANDSAT8_L1': 'LANDSAT_OT_L1',
    'LANDSAT8_L2': 'LANDSAT_OT_L2'
}


class DataCollection(Enum, metaclass=_DataCollectionMeta):
    """ An enum class for data collections

    It contains a number of predefined data collections, which are the most commonly used with Sentinel Hub service.
    Additionally it also allows defining new data collections by specifying data collection parameters relevant for
    the service. Check `DataCollection.define` and similar methods for more.
    """
    SENTINEL2_L1C = DataCollectionDefinition(
        api_id='sentinel-2-l1c',
        catalog_id='sentinel-2-l1c',
        wfs_id='DSS1',
        service_url=ServiceUrl.MAIN,
        collection_type=_CollectionType.SENTINEL2,
        sensor_type=_SensorType.MSI,
        processing_level=_ProcessingLevel.L1C,
        bands=_Bands.SENTINEL2_L1C,
        has_cloud_coverage=True
    )
    SENTINEL2_L2A = DataCollectionDefinition(
        api_id='sentinel-2-l2a',
        catalog_id='sentinel-2-l2a',
        wfs_id='DSS2',
        service_url=ServiceUrl.MAIN,
        collection_type=_CollectionType.SENTINEL2,
        sensor_type=_SensorType.MSI,
        processing_level=_ProcessingLevel.L2A,
        bands=_Bands.SENTINEL2_L2A,
        has_cloud_coverage=True
    )

    SENTINEL1 = DataCollectionDefinition(
        api_id='sentinel-1-grd',
        catalog_id='sentinel-1-grd',
        wfs_id='DSS3',
        service_url=ServiceUrl.MAIN,
        collection_type=_CollectionType.SENTINEL1,
        sensor_type=_SensorType.C_SAR,
        processing_level=_ProcessingLevel.GRD,
        orbit_direction=OrbitDirection.BOTH
    )
    SENTINEL1_IW = SENTINEL1.derive(
        swath_mode=_SwathMode.IW,
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
        swath_mode=_SwathMode.EW,
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

    DEM = DataCollectionDefinition(
        api_id='dem',
        service_url=ServiceUrl.MAIN,
        collection_type=_CollectionType.DEM,
        bands=_Bands.DEM,
        is_timeless=True
    )
    DEM_MAPZEN = DEM.derive(
        dem_instance='MAPZEN'
    )
    DEM_COPERNICUS_30 = DEM.derive(
        dem_instance='COPERNICUS_30'
    )
    DEM_COPERNICUS_90 = DEM.derive(
        dem_instance='COPERNICUS_90'
    )

    MODIS = DataCollectionDefinition(
        api_id='modis',
        catalog_id='modis',
        wfs_id='DSS5',
        service_url=ServiceUrl.USWEST,
        collection_type=_CollectionType.MODIS,
        bands=_Bands.MODIS
    )

    LANDSAT_MSS_L1 = DataCollectionDefinition(
        api_id='landsat-mss-l1',
        catalog_id='landsat-mss-l1',
        wfs_id='DSS14',
        service_url=ServiceUrl.USWEST,
        collection_type=_CollectionType.LANDSAT_MSS,
        sensor_type=_SensorType.MSS,
        processing_level=_ProcessingLevel.L1,
        bands=_Bands.LANDSAT_MSS,
        has_cloud_coverage=True
    )

    LANDSAT_TM_L1 = DataCollectionDefinition(
        api_id='landsat-tm-l1',
        catalog_id='landsat-tm-l1',
        wfs_id='DSS15',
        service_url=ServiceUrl.USWEST,
        collection_type=_CollectionType.LANDSAT_TM,
        sensor_type=_SensorType.TM,
        processing_level=_ProcessingLevel.L1,
        bands=_Bands.LANDSAT_TM,
        has_cloud_coverage=True
    )
    LANDSAT_TM_L2 = LANDSAT_TM_L1.derive(
        api_id='landsat-tm-l2',
        catalog_id='landsat-tm-l2',
        wfs_id='DSS16',
        processing_level=_ProcessingLevel.L2
    )

    LANDSAT_ETM_L1 = DataCollectionDefinition(
        api_id='landsat-etm-l1',
        catalog_id='landsat-etm-l1',
        wfs_id='DSS17',
        service_url=ServiceUrl.USWEST,
        collection_type=_CollectionType.LANDSAT_ETM,
        sensor_type=_SensorType.ETM,
        processing_level=_ProcessingLevel.L1,
        bands=_Bands.LANDSAT_ETM_L1,
        has_cloud_coverage=True
    )
    LANDSAT_ETM_L2 = LANDSAT_ETM_L1.derive(
        api_id='landsat-etm-l2',
        catalog_id='landsat-etm-l2',
        wfs_id='DSS18',
        processing_level=_ProcessingLevel.L2,
        bands=_Bands.LANDSAT_ETM_L2
    )

    LANDSAT_OT_L1 = DataCollectionDefinition(
        api_id='landsat-ot-l1',
        catalog_id='landsat-ot-l1',
        wfs_id='DSS12',
        service_url=ServiceUrl.USWEST,
        collection_type=_CollectionType.LANDSAT_OT,
        sensor_type=_SensorType.OLI_TIRS,
        processing_level=_ProcessingLevel.L1,
        bands=_Bands.LANDSAT_OT,
        has_cloud_coverage=True
    )
    LANDSAT_OT_L2 = LANDSAT_OT_L1.derive(
        api_id='landsat-ot-l2',
        catalog_id='landsat-ot-l2',
        wfs_id='DSS13',
        processing_level=_ProcessingLevel.L2,
        bands=_Bands.LANDSAT_OT_L2
    )

    SENTINEL5P = DataCollectionDefinition(
        api_id='sentinel-5p-l2',
        catalog_id='sentinel-5p-l2',
        wfs_id='DSS7',
        service_url=ServiceUrl.CREODIAS,
        collection_type=_CollectionType.SENTINEL5P,
        sensor_type=_SensorType.TROPOMI,
        processing_level=_ProcessingLevel.L2,
        bands=_Bands.SENTINEL5P
    )
    SENTINEL3_OLCI = DataCollectionDefinition(
        api_id='sentinel-3-olci',
        catalog_id='sentinel-3-olci',
        wfs_id='DSS8',
        service_url=ServiceUrl.CREODIAS,
        collection_type=_CollectionType.SENTINEL3,
        sensor_type=_SensorType.OLCI,
        processing_level=_ProcessingLevel.L1B,
        bands=_Bands.SENTINEL3_OLCI
    )
    SENTINEL3_SLSTR = DataCollectionDefinition(
        api_id='sentinel-3-slstr',
        catalog_id='sentinel-3-slstr',
        wfs_id='DSS9',
        service_url=ServiceUrl.CREODIAS,
        collection_type=_CollectionType.SENTINEL3,
        sensor_type=_SensorType.SLSTR,
        processing_level=_ProcessingLevel.L1B,
        bands=_Bands.SENTINEL3_SLSTR,
        has_cloud_coverage=True
    )

    # EOCloud collections (which are only available on a development eocloud service):
    LANDSAT5 = DataCollectionDefinition(
        wfs_id='L5.TILE',
        service_url=ServiceUrl.EOCLOUD,
        processing_level=_ProcessingLevel.GRD
    )
    LANDSAT7 = DataCollectionDefinition(
        wfs_id='L7.TILE',
        service_url=ServiceUrl.EOCLOUD,
        processing_level=_ProcessingLevel.GRD
    )
    ENVISAT_MERIS = DataCollectionDefinition(
        wfs_id='ENV.TILE',
        service_url=ServiceUrl.EOCLOUD,
        collection_type=_CollectionType.ENVISAT_MERIS
    )

    @classmethod
    def define(cls, name, *, api_id=None, catalog_id=None, wfs_id=None, service_url=None, collection_type=None,
               sensor_type=None, processing_level=None, swath_mode=None, polarization=None, resolution=None,
               orbit_direction=None, timeliness=None, bands=None, collection_id=None, is_timeless=False,
               has_cloud_coverage=False, dem_instance=None):
        """ Define a new data collection

        Note that all parameters, except `name` are optional. If a data collection definition won't be used for a
        certain use case (e.g. Process API, WFS, etc.), parameters for that use case don't have to be defined

        :param name: A name of a new data collection
        :type name: str
        :param api_id: An ID to be used for Sentinel Hub Process API
        :type api_id: str or None
        :param catalog_id: An ID to be used for Sentinel Hub Catalog API
        :type catalog_id: str or None
        :param wfs_id: An ID to be used for Sentinel Hub WFS service
        :type wfs_id: str or None
        :param service_url: A base URL of Sentinel Hub service deployment from where to download data. If it is not
            specified, a `sh_base_url` from a config will be used by default
        :type service_url: str or None
        :param collection_type: A collection type
        :type collection_type: str or None
        :param sensor_type: A type of a satellite's sensor
        :type sensor_type: str or None
        :param processing_level: A level of processing applied on satellite data
        :type processing_level: str or None
        :param swath_mode: A swath mode of SAR sensors
        :type swath_mode: str or None
        :param polarization: A type of polarization
        :type polarization: str or None
        :param resolution: A type of (Sentinel-1) resolution
        :type resolution: str or None
        :param orbit_direction: A direction of satellite's orbit by which to filter satellite's data
        :type orbit_direction: str or None
        :param timeliness: A timeliness of data
        :type timeliness: str or None
        :param bands: Names of data collection bands
        :type bands: tuple(str) or None
        :param collection_id: An ID of a BYOC or BATCH collection
        :type collection_id: str or None
        :param is_timeless: `True` if a data collection can be filtered by time dimension and `False` otherwise
        :type is_timeless: bool
        :param has_cloud_coverage: `True` if data collection can be filtered by cloud coverage percentage and `False`
            otherwise
        :type has_cloud_coverage: bool
        :param dem_instance: one of the options listed in
            `DEM documentation <https://docs.sentinel-hub.com/api/latest/data/dem/#deminstance>`__
        :type dem_instance: str or None
        :return: A new data collection
        :rtype: DataCollection
        """
        definition = DataCollectionDefinition(
            api_id=api_id,
            catalog_id=catalog_id,
            wfs_id=wfs_id,
            service_url=service_url,
            collection_type=collection_type,
            sensor_type=sensor_type,
            processing_level=processing_level,
            swath_mode=swath_mode,
            polarization=polarization,
            resolution=resolution,
            orbit_direction=orbit_direction,
            timeliness=timeliness,
            bands=bands,
            collection_id=collection_id,
            is_timeless=is_timeless,
            has_cloud_coverage=has_cloud_coverage,
            dem_instance=dem_instance,
            _name=name
        )
        cls._try_add_data_collection(name, definition)
        return cls(definition)

    def define_from(self, name, **params):
        """ Define a new data collection from an existing one

        :param name: A name of a new data collection
        :type name: str
        :param params: Any parameter to override current data collection parameters
        :return: A new data collection
        :rtype: DataCollection
        """
        definition = self.value
        new_definition = definition.derive(**params, _name=name)

        self._try_add_data_collection(name, new_definition)
        return DataCollection(new_definition)

    @classmethod
    def _try_add_data_collection(cls, name, definition):
        """ Tries adding a new data collection definition. If the exact enum has already been defined then it won't do
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
            raise ValueError(f"Data collection name '{name}' is already taken by another data collection")

        existing_collection = cls._value2member_map_[definition]
        raise ValueError(f'Data collection definition is already taken by {existing_collection}. Two different '
                         f'DataCollection enums cannot have the same definition.')

    @classmethod
    def define_byoc(cls, collection_id, **params):
        """ Defines a BYOC data collection

        :param collection_id: An ID of a data collection
        :type collection_id: str
        :param params: Any parameter to override default BYOC data collection parameters
        :return: A new data collection
        :rtype: DataCollection
        """
        params['name'] = params.get('name', f'BYOC_{collection_id}')
        params['api_id'] = params.get('api_id', f'byoc-{collection_id}')
        params['catalog_id'] = params.get('catalog_id', f'byoc-{collection_id}')
        params['wfs_id'] = params.get('wfs_id', f'byoc-{collection_id}')
        params['collection_type'] = params.get('collection_type', _CollectionType.BYOC)
        params['collection_id'] = collection_id
        return cls.define(**params)

    @classmethod
    def define_batch(cls, collection_id, **params):
        """ Defines a BATCH data collection

        :param collection_id: An ID of a data collection
        :type collection_id: str
        :param params: Any parameter to override default BATCH data collection parameters
        :return: A new data collection
        :rtype: DataCollection
        """
        params['name'] = params.get('name', f'BATCH_{collection_id}')
        params['api_id'] = params.get('api_id', f'batch-{collection_id}')
        params['catalog_id'] = params.get('catalog_id', f'batch-{collection_id}')
        params['wfs_id'] = params.get('wfs_id', f'batch-{collection_id}')
        params['collection_type'] = params.get('collection_type', _CollectionType.BATCH)
        params['collection_id'] = collection_id
        return cls.define(**params)

    @property
    def api_id(self):
        """ Provides a Sentinel Hub Process API identifier or raises an error if it is not defined

        :return: An identifier
        :rtype: str
        :raises: ValueError
        """
        if self.value.api_id is None:
            raise ValueError(f'Data collection {self.name} is missing a Sentinel Hub Process API identifier')
        return self.value.api_id

    @property
    def catalog_id(self):
        """ Provides a Sentinel Hub Catalog API identifier or raises an error if it is not defined

        :return: An identifier
        :rtype: str
        :raises: ValueError
        """
        if self.value.catalog_id is None:
            raise ValueError(f'Data collection {self.name} is missing a Sentinel Hub Catalog API identifier')
        return self.value.catalog_id

    @property
    def wfs_id(self):
        """ Provides a Sentinel Hub WFS identifier or raises an error if it is not defined

        :return: An identifier
        :rtype: str
        :raises: ValueError
        """
        if self.value.wfs_id is None:
            raise ValueError(f'Data collection {self.name} is missing a Sentinel Hub WFS identifier')
        return self.value.wfs_id

    @property
    def bands(self):
        """ Provides band names available for the data collection

        :return: A tuple of band names
        :rtype: tuple(str)
        :raises: ValueError
        """
        if self.value.bands is None:
            raise ValueError(f'Data collection {self.name} does not define bands')
        return self.value.bands

    def __getattr__(self, item, *args, **kwargs):
        """ The following insures that any attribute from DataCollectionDefinition, which is already not a
        property or an attribute of DataCollection, becomes an attribute of DataCollection
        """
        if not item.startswith('_') and hasattr(self, 'value') and isinstance(self.value, DataCollectionDefinition):
            definition_dict = asdict(self.value)
            if item in definition_dict:
                return definition_dict[item]

        return super().__getattribute__(item, *args, **kwargs)

    @property
    def is_sentinel1(self):
        """ Checks if data collection is a Sentinel-1 collection type

        Example: ``DataCollection.SENTINEL1_IW.is_sentinel1``

        :return: `True` if collection is Sentinel-1 collection type and `False` otherwise
        :rtype: bool
        """
        return self.collection_type == _CollectionType.SENTINEL1

    def contains_orbit_direction(self, orbit_direction):
        """ Checks if a data collection contains given orbit direction

        :param orbit_direction: An orbit direction
        :type orbit_direction: string
        :return: `True` if data collection contains the orbit direction
        :return: bool
        """
        defined_direction = self.orbit_direction
        if defined_direction is None or defined_direction.upper() == OrbitDirection.BOTH:
            return True
        return orbit_direction.upper() == defined_direction.upper()

    @classmethod
    def get_available_collections(cls, config=None):
        """ Returns which data collections are available for configured Sentinel Hub OGC URL

        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        :return: List of available data collections
        :rtype: list(DataCollection)
        """
        config = config or SHConfig()
        is_eocloud = config.has_eocloud_url()

        return [data_collection for data_collection in cls
                if (data_collection.service_url == ServiceUrl.EOCLOUD) == is_eocloud]


def _raise_invalid_id(collection_id):
    """ Checks if a given collection ID conforms to an expected pattern and raises an error if it doesn't
    """
    collection_id_pattern = '.{8}-.{4}-.{4}-.{4}-.{12}'

    if not re.compile(collection_id_pattern).match(collection_id):
        raise ValueError(f'Given collection id does not match the expected format {collection_id_pattern}')


DataSource = DataCollection


def handle_deprecated_data_source(data_collection, data_source, default=None):
    """ Joins parameters used to specify a data collection. In case data_source is given it raises a warning. In case
    both are given it raises an error. In case neither are given but there is a default collection it raises another
    warning.

    Note that this function is only temporary and will be removed in future package versions
    """
    if data_source is not None:
        warnings.warn('Parameter data_source is deprecated, use data_collection instead',
                      category=SHDeprecationWarning)

    if data_collection is not None and data_source is not None:
        raise ValueError('Only one of the parameters data_collection and data_source should be given')

    if data_collection is None and data_source is None and default is not None:
        warnings.warn('In the future please specify data_collection parameter, for now taking '
                      'DataCollection.SENTINEL2_L1C', category=SHDeprecationWarning)
        return default

    return data_collection or data_source

"""
Module defining data collections
"""
from dataclasses import dataclass, field, fields
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from aenum import extend_enum

from .constants import ServiceUrl
from .data_collections_bands import Band, Bands, MetaBands

if TYPE_CHECKING:
    from enum import Enum, EnumMeta  # mypy has a custom plugin for enum but not aenum
else:
    from aenum import Enum, EnumMeta


class _CollectionType:
    """Types of Sentinel Hub data collections"""

    SENTINEL1 = "Sentinel-1"
    SENTINEL2 = "Sentinel-2"
    SENTINEL3 = "Sentinel-3"
    SENTINEL5P = "Sentinel-5P"
    LANDSAT_MSS = "Landsat 1-5 MSS"
    LANDSAT_TM = "Landsat 4-5 TM"
    LANDSAT5 = "Landsat 5"
    LANDSAT_ETM = "Landsat 7 ETM+"
    LANDSAT_OT = "Landsat 8 OLI and TIRS"
    MODIS = "MODIS"
    ENVISAT_MERIS = "Envisat Meris"
    DEM = "DEM"
    BYOC = "BYOC"
    BATCH = "BATCH"
    HLS = "Harmonized Landsat Sentinel"


class _SensorType:
    """Satellite sensors"""

    # pylint: disable=invalid-name
    MSI = "MSI"
    OLI_TIRS = "OLI-TIRS"
    TM = "TM"
    ETM = "ETM+"
    MSS = "MSS"
    C_SAR = "C-SAR"
    OLCI = "OLCI"
    SLSTR = "SLSTR"
    TROPOMI = "TROPOMI"


class _ProcessingLevel:
    """Processing levels"""

    # pylint: disable=invalid-name
    L1 = "L1"
    L2 = "L2"
    L1B = "L1B"
    L1C = "L1C"
    L2A = "L2A"
    L3B = "L3B"
    GRD = "GRD"


class _SwathMode:
    """Swath modes for SAR sensors"""

    # pylint: disable=invalid-name
    IW = "IW"
    EW = "EW"
    SM = "SM"
    WV = "WV"


class _Polarization:
    """SAR polarizations"""

    # pylint: disable=invalid-name
    DV = "DV"
    DH = "DH"
    SV = "SV"
    SH = "SH"
    HH = "HH"
    HV = "HV"
    VV = "VV"
    VH = "VH"


class _Resolution:
    """Product resolution (specific to Sentinel-1 collections)"""

    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class OrbitDirection:
    """Orbit directions"""

    ASCENDING = "ASCENDING"
    DESCENDING = "DESCENDING"
    BOTH = "BOTH"


def _shallow_asdict(dataclass_instance: Any) -> Dict[str, Any]:
    """Returns a dictionary of fields and values, but is not recursive and does not deepcopy like `asdict`"""
    # This definition needs to be above the class definitions in the file
    return {field.name: getattr(dataclass_instance, field.name) for field in fields(dataclass_instance)}


class _DataCollectionMeta(EnumMeta):
    """Metaclass that builds DataCollection class enums"""

    def __call__(cls, value, *args, **kwargs):  # type: ignore[no-untyped-def]
        """This is executed whenever `DataCollection('something')` is called

        This solves a problem of pickling a custom DataCollection and unpickling it in another process
        """
        if isinstance(value, DataCollectionDefinition) and value not in cls._value2member_map_ and value._name:
            cls._try_add_data_collection(value._name, value)

        return super().__call__(value, *args, **kwargs)


@dataclass(frozen=True)
class DataCollectionDefinition:
    """An immutable definition of a data collection

    Check `DataCollection.define` for more info about attributes of this class
    """

    # pylint: disable=too-many-instance-attributes
    api_id: Optional[str] = None
    catalog_id: Optional[str] = None
    wfs_id: Optional[str] = None
    service_url: Optional[str] = None
    collection_type: Optional[str] = None
    sensor_type: Optional[str] = None
    processing_level: Optional[str] = None
    swath_mode: Optional[str] = None
    polarization: Optional[str] = None
    resolution: Optional[str] = None
    orbit_direction: Optional[str] = None
    timeliness: Optional[str] = None
    bands: Optional[Tuple[Band, ...]] = None
    metabands: Optional[Tuple[Band, ...]] = None
    collection_id: Optional[str] = None
    is_timeless: bool = False
    has_cloud_coverage: bool = False
    dem_instance: Optional[str] = None
    # The following parameter is used to preserve custom DataCollection name during pickling and unpickling process:
    _name: Optional[str] = field(default=None, compare=False)

    def __post_init__(self):  # type: ignore[no-untyped-def] # not typechecked because we mutate immutable dataclasses
        """In case a list of bands or metabands has been given this makes sure to cast it into a tuple"""
        if isinstance(self.bands, list):
            object.__setattr__(self, "bands", tuple(self.bands))
        if isinstance(self.metabands, list):
            object.__setattr__(self, "metabands", tuple(self.metabands))

    def __repr__(self) -> str:
        """A nicer representation of parameters that define a data collection"""
        valid_params = {name: value for name, value in _shallow_asdict(self).items() if value is not None}
        params_repr = "\n  ".join(f"{name}: {value}" for name, value in valid_params.items() if name != "_name")
        return f"{self.__class__.__name__}(\n  {params_repr}\n)"

    def derive(self, **params: Any) -> "DataCollectionDefinition":
        """Create a new data collection definition from current definition and parameters that override current
        parameters

        :param params: Any of DataCollectionDefinition attributes
        :return: A new data collection definition
        """
        derived_params = _shallow_asdict(self)
        derived_params.update(params)

        return DataCollectionDefinition(**derived_params)


class DataCollection(Enum, metaclass=_DataCollectionMeta):
    """An enum class for data collections

    It contains a number of predefined data collections, which are the most commonly used with Sentinel Hub service.
    Additionally, it also allows defining new data collections by specifying data collection parameters relevant for
    the service. Check `DataCollection.define` and similar methods for more.
    """

    SENTINEL2_L1C = DataCollectionDefinition(
        api_id="sentinel-2-l1c",
        catalog_id="sentinel-2-l1c",
        wfs_id="DSS1",
        service_url=ServiceUrl.MAIN,
        collection_type=_CollectionType.SENTINEL2,
        sensor_type=_SensorType.MSI,
        processing_level=_ProcessingLevel.L1C,
        bands=Bands.SENTINEL2_L1C,
        metabands=MetaBands.SENTINEL2_L1C,
        has_cloud_coverage=True,
    )
    SENTINEL2_L2A = DataCollectionDefinition(
        api_id="sentinel-2-l2a",
        catalog_id="sentinel-2-l2a",
        wfs_id="DSS2",
        service_url=ServiceUrl.MAIN,
        collection_type=_CollectionType.SENTINEL2,
        sensor_type=_SensorType.MSI,
        processing_level=_ProcessingLevel.L2A,
        bands=Bands.SENTINEL2_L2A,
        metabands=MetaBands.SENTINEL2_L2A,
        has_cloud_coverage=True,
    )

    SENTINEL1 = DataCollectionDefinition(
        api_id="sentinel-1-grd",
        catalog_id="sentinel-1-grd",
        wfs_id="DSS3",
        service_url=ServiceUrl.MAIN,
        collection_type=_CollectionType.SENTINEL1,
        sensor_type=_SensorType.C_SAR,
        processing_level=_ProcessingLevel.GRD,
        orbit_direction=OrbitDirection.BOTH,
        metabands=MetaBands.SENTINEL1,
    )
    SENTINEL1_IW = SENTINEL1.derive(
        swath_mode=_SwathMode.IW,
        polarization=_Polarization.DV,
        resolution=_Resolution.HIGH,
        bands=Bands.SENTINEL1_IW,
    )
    SENTINEL1_IW_ASC = SENTINEL1_IW.derive(
        orbit_direction=OrbitDirection.ASCENDING,
    )
    SENTINEL1_IW_DES = SENTINEL1_IW.derive(
        orbit_direction=OrbitDirection.DESCENDING,
    )
    SENTINEL1_EW = SENTINEL1.derive(
        swath_mode=_SwathMode.EW,
        polarization=_Polarization.DH,
        resolution=_Resolution.MEDIUM,
        bands=Bands.SENTINEL1_EW,
    )
    SENTINEL1_EW_ASC = SENTINEL1_EW.derive(
        orbit_direction=OrbitDirection.ASCENDING,
    )
    SENTINEL1_EW_DES = SENTINEL1_EW.derive(
        orbit_direction=OrbitDirection.DESCENDING,
    )
    SENTINEL1_EW_SH = SENTINEL1_EW.derive(
        polarization=_Polarization.SH,
        bands=Bands.SENTINEL1_EW_SH,
    )
    SENTINEL1_EW_SH_ASC = SENTINEL1_EW_SH.derive(
        orbit_direction=OrbitDirection.ASCENDING,
    )
    SENTINEL1_EW_SH_DES = SENTINEL1_EW_SH.derive(
        orbit_direction=OrbitDirection.DESCENDING,
    )

    DEM = DataCollectionDefinition(
        api_id="dem",
        service_url=ServiceUrl.MAIN,
        collection_type=_CollectionType.DEM,
        bands=Bands.DEM,
        metabands=MetaBands.DEM,
        is_timeless=True,
    )
    DEM_MAPZEN = DEM.derive(
        dem_instance="MAPZEN",
    )
    DEM_COPERNICUS_30 = DEM.derive(
        dem_instance="COPERNICUS_30",
    )
    DEM_COPERNICUS_90 = DEM.derive(
        dem_instance="COPERNICUS_90",
    )

    MODIS = DataCollectionDefinition(
        api_id="modis",
        catalog_id="modis",
        wfs_id="DSS5",
        service_url=ServiceUrl.USWEST,
        collection_type=_CollectionType.MODIS,
        bands=Bands.MODIS,
        metabands=MetaBands.MODIS,
    )

    LANDSAT_MSS_L1 = DataCollectionDefinition(
        api_id="landsat-mss-l1",
        catalog_id="landsat-mss-l1",
        wfs_id="DSS14",
        service_url=ServiceUrl.USWEST,
        collection_type=_CollectionType.LANDSAT_MSS,
        sensor_type=_SensorType.MSS,
        processing_level=_ProcessingLevel.L1,
        bands=Bands.LANDSAT_MSS_L1,
        metabands=MetaBands.LANDSAT_MSS_L1,
        has_cloud_coverage=True,
    )

    LANDSAT_TM_L1 = DataCollectionDefinition(
        api_id="landsat-tm-l1",
        catalog_id="landsat-tm-l1",
        wfs_id="DSS15",
        service_url=ServiceUrl.USWEST,
        collection_type=_CollectionType.LANDSAT_TM,
        sensor_type=_SensorType.TM,
        processing_level=_ProcessingLevel.L1,
        bands=Bands.LANDSAT_TM_L1,
        metabands=MetaBands.LANDSAT_TM_L1,
        has_cloud_coverage=True,
    )
    LANDSAT_TM_L2 = LANDSAT_TM_L1.derive(
        api_id="landsat-tm-l2",
        catalog_id="landsat-tm-l2",
        wfs_id="DSS16",
        processing_level=_ProcessingLevel.L2,
        bands=Bands.LANDSAT_TM_L2,
        metabands=MetaBands.LANDSAT_TM_L2,
    )

    LANDSAT_ETM_L1 = DataCollectionDefinition(
        api_id="landsat-etm-l1",
        catalog_id="landsat-etm-l1",
        wfs_id="DSS17",
        service_url=ServiceUrl.USWEST,
        collection_type=_CollectionType.LANDSAT_ETM,
        sensor_type=_SensorType.ETM,
        processing_level=_ProcessingLevel.L1,
        bands=Bands.LANDSAT_ETM_L1,
        metabands=MetaBands.LANDSAT_ETM_L1,
        has_cloud_coverage=True,
    )
    LANDSAT_ETM_L2 = LANDSAT_ETM_L1.derive(
        api_id="landsat-etm-l2",
        catalog_id="landsat-etm-l2",
        wfs_id="DSS18",
        processing_level=_ProcessingLevel.L2,
        bands=Bands.LANDSAT_ETM_L2,
        metabands=MetaBands.LANDSAT_ETM_L2,
    )

    LANDSAT_OT_L1 = DataCollectionDefinition(
        api_id="landsat-ot-l1",
        catalog_id="landsat-ot-l1",
        wfs_id="DSS12",
        service_url=ServiceUrl.USWEST,
        collection_type=_CollectionType.LANDSAT_OT,
        sensor_type=_SensorType.OLI_TIRS,
        processing_level=_ProcessingLevel.L1,
        bands=Bands.LANDSAT_OT_L1,
        metabands=MetaBands.LANDSAT_OT_L1,
        has_cloud_coverage=True,
    )
    LANDSAT_OT_L2 = LANDSAT_OT_L1.derive(
        api_id="landsat-ot-l2",
        catalog_id="landsat-ot-l2",
        wfs_id="DSS13",
        processing_level=_ProcessingLevel.L2,
        bands=Bands.LANDSAT_OT_L2,
        metabands=MetaBands.LANDSAT_OT_L2,
    )

    SENTINEL5P = DataCollectionDefinition(
        api_id="sentinel-5p-l2",
        catalog_id="sentinel-5p-l2",
        wfs_id="DSS7",
        service_url=ServiceUrl.CREODIAS,
        collection_type=_CollectionType.SENTINEL5P,
        sensor_type=_SensorType.TROPOMI,
        processing_level=_ProcessingLevel.L2,
        bands=Bands.SENTINEL5P,
        metabands=MetaBands.SENTINEL5P,
    )
    SENTINEL3_OLCI = DataCollectionDefinition(
        api_id="sentinel-3-olci",
        catalog_id="sentinel-3-olci",
        wfs_id="DSS8",
        service_url=ServiceUrl.CREODIAS,
        collection_type=_CollectionType.SENTINEL3,
        sensor_type=_SensorType.OLCI,
        processing_level=_ProcessingLevel.L1B,
        bands=Bands.SENTINEL3_OLCI,
        metabands=MetaBands.SENTINEL3_OLCI,
    )
    SENTINEL3_SLSTR = DataCollectionDefinition(
        api_id="sentinel-3-slstr",
        catalog_id="sentinel-3-slstr",
        wfs_id="DSS9",
        service_url=ServiceUrl.CREODIAS,
        collection_type=_CollectionType.SENTINEL3,
        sensor_type=_SensorType.SLSTR,
        processing_level=_ProcessingLevel.L1B,
        bands=Bands.SENTINEL3_SLSTR,
        metabands=MetaBands.SENTINEL3_SLSTR,
        has_cloud_coverage=True,
    )
    HARMONIZED_LANDSAT_SENTINEL = DataCollectionDefinition(
        api_id="hls",
        catalog_id="hls",
        collection_type=_CollectionType.HLS,
        service_url=ServiceUrl.USWEST,
        bands=Bands.HARMONIZED_LANDSAT_SENTINEL,
        metabands=MetaBands.HARMONIZED_LANDSAT_SENTINEL,
        has_cloud_coverage=True,
    )

    # pylint: disable=too-many-locals
    @classmethod
    def define(
        cls,
        name: str,
        *,
        api_id: Optional[str] = None,
        catalog_id: Optional[str] = None,
        wfs_id: Optional[str] = None,
        service_url: Optional[str] = None,
        collection_type: Optional[str] = None,
        sensor_type: Optional[str] = None,
        processing_level: Optional[str] = None,
        swath_mode: Optional[str] = None,
        polarization: Optional[str] = None,
        resolution: Optional[str] = None,
        orbit_direction: Optional[str] = None,
        timeliness: Optional[str] = None,
        bands: Optional[Tuple[Band, ...]] = None,
        metabands: Optional[Tuple[Band, ...]] = None,
        collection_id: Optional[str] = None,
        is_timeless: bool = False,
        has_cloud_coverage: bool = False,
        dem_instance: Optional[str] = None,
    ) -> "DataCollection":
        """Define a new data collection

        Note that all parameters, except `name` are optional. If a data collection definition won't be used for a
        certain use case (e.g. Process API, WFS, etc.), parameters for that use case don't have to be defined

        :param name: A name of a new data collection
        :param api_id: An ID to be used for Sentinel Hub Process API
        :param catalog_id: An ID to be used for Sentinel Hub Catalog API
        :param wfs_id: An ID to be used for Sentinel Hub WFS service
        :param service_url: A base URL of Sentinel Hub service deployment from where to download data. If it is not
            specified, a `sh_base_url` from a config will be used by default
        :param collection_type: A collection type
        :param sensor_type: A type of a satellite's sensor
        :param processing_level: A level of processing applied on satellite data
        :param swath_mode: A swath mode of SAR sensors
        :param polarization: A type of polarization
        :param resolution: A type of (Sentinel-1) resolution
        :param orbit_direction: A direction of satellite's orbit by which to filter satellite's data
        :param timeliness: A timeliness of data
        :param bands: Information about data collection bands
        :param metabands: Information about data collection metabands
        :param collection_id: An ID of a BYOC or BATCH collection
        :param is_timeless: `True` if a data collection can be filtered by time dimension and `False` otherwise
        :param has_cloud_coverage: `True` if data collection can be filtered by cloud coverage percentage and `False`
            otherwise
        :param dem_instance: one of the options listed in
            `DEM documentation <https://docs.sentinel-hub.com/api/latest/data/dem/#deminstance>`__
        :return: A new data collection
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
            metabands=metabands,
            collection_id=collection_id,
            is_timeless=is_timeless,
            has_cloud_coverage=has_cloud_coverage,
            dem_instance=dem_instance,
            _name=name,
        )
        cls._try_add_data_collection(name, definition)
        return cls(definition)

    def define_from(self, name: str, **params: Any) -> "DataCollection":
        """Define a new data collection from an existing one

        :param name: A name of a new data collection
        :param params: Any parameter to override current data collection parameters
        :return: A new data collection
        """
        definition = self.value
        new_definition = definition.derive(**params, _name=name)

        self._try_add_data_collection(name, new_definition)
        return DataCollection(new_definition)

    @classmethod
    def _try_add_data_collection(cls, name: str, definition: DataCollectionDefinition) -> None:
        """Tries adding a new data collection definition. If the exact enum has already been defined then it won't do
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
            raise ValueError(f"Data collection name `{name}` is already taken by another data collection")

        existing_collection = cls._value2member_map_[definition]
        raise ValueError(
            f"Data collection definition is already taken by {existing_collection}. Two different "
            "DataCollection enums cannot have the same definition."
        )

    @classmethod
    def define_byoc(cls, collection_id: str, **params: Any) -> "DataCollection":
        """Defines a BYOC data collection

        :param collection_id: An ID of a data collection
        :param params: Any parameter to override default BYOC data collection parameters
        :return: A new data collection
        """
        params["name"] = params.get("name", f"BYOC_{collection_id}")
        params["api_id"] = params.get("api_id", f"byoc-{collection_id}")
        params["catalog_id"] = params.get("catalog_id", f"byoc-{collection_id}")
        params["wfs_id"] = params.get("wfs_id", f"byoc-{collection_id}")
        params["collection_type"] = params.get("collection_type", _CollectionType.BYOC)
        params["collection_id"] = collection_id
        return cls.define(**params)

    @classmethod
    def define_batch(cls, collection_id: str, **params: Any) -> "DataCollection":
        """Defines a BATCH data collection

        :param collection_id: An ID of a data collection
        :param params: Any parameter to override default BATCH data collection parameters
        :return: A new data collection
        """
        params["name"] = params.get("name", f"BATCH_{collection_id}")
        params["api_id"] = params.get("api_id", f"batch-{collection_id}")
        params["catalog_id"] = params.get("catalog_id", f"batch-{collection_id}")
        params["wfs_id"] = params.get("wfs_id", f"batch-{collection_id}")
        params["collection_type"] = params.get("collection_type", _CollectionType.BATCH)
        params["collection_id"] = collection_id
        return cls.define(**params)

    @property
    def api_id(self) -> str:
        """Provides a Sentinel Hub Process API identifier or raises an error if it is not defined

        :return: An identifier
        :raises: ValueError
        """
        if self.value.api_id is None:
            raise ValueError(f"Data collection {self.name} is missing a Sentinel Hub Process API identifier")
        return self.value.api_id

    @property
    def catalog_id(self) -> str:
        """Provides a Sentinel Hub Catalog API identifier or raises an error if it is not defined

        :return: An identifier
        :raises: ValueError
        """
        if self.value.catalog_id is not None:
            return self.value.catalog_id
        if self.value.api_id is not None:
            # A fallback because Process API and Catalog API IDs should now be unified
            return self.value.api_id
        raise ValueError(f"Data collection {self.name} is missing a Sentinel Hub Catalog API identifier")

    @property
    def wfs_id(self) -> str:
        """Provides a Sentinel Hub WFS identifier or raises an error if it is not defined

        :return: An identifier
        :raises: ValueError
        """
        if self.value.wfs_id is None:
            raise ValueError(f"Data collection {self.name} is missing a Sentinel Hub WFS identifier")
        return self.value.wfs_id

    @property
    def bands(self) -> Tuple[Band, ...]:
        """Provides band information available for the data collection

        :return: A tuple of band info
        :raises: ValueError
        """
        if self.value.bands is None:
            raise ValueError(f"Data collection {self.name} does not define bands")
        return self.value.bands

    @property
    def metabands(self) -> Tuple[Band, ...]:
        """Provides metaband information available for the data collection

        :return: A tuple of metaband info
        :raises: ValueError
        """
        if self.value.metabands is None:
            raise ValueError(f"Data collection {self.name} does not define metabands")
        return self.value.metabands

    def __getattr__(self, item: str) -> Any:
        """The following insures that any attribute from DataCollectionDefinition, which is already not a
        property or an attribute of DataCollection, becomes an attribute of DataCollection
        """
        if not item.startswith("_") and hasattr(self, "value") and isinstance(self.value, DataCollectionDefinition):
            definition_dict = _shallow_asdict(self.value)
            if item in definition_dict:
                return definition_dict[item]

        return super().__getattribute__(item)

    @property
    def is_sentinel1(self) -> bool:
        """Checks if data collection is a Sentinel-1 collection type

        Example: ``DataCollection.SENTINEL1_IW.is_sentinel1``

        :return: `True` if collection is Sentinel-1 collection type and `False` otherwise
        """
        return self.collection_type == _CollectionType.SENTINEL1

    @property
    def is_byoc(self) -> bool:
        """Checks if data collection is a BYOC collection type

        :return: `True` if collection is a BYOC collection type and `False` otherwise
        """
        return self.collection_type == _CollectionType.BYOC

    @property
    def is_batch(self) -> bool:
        """Checks if data collection is a batch collection type

        :return: `True` if collection is a batch collection type and `False` otherwise
        """
        return self.collection_type == _CollectionType.BATCH

    def contains_orbit_direction(self, orbit_direction: str) -> bool:
        """Checks if a data collection contains given orbit direction

        :param orbit_direction: An orbit direction
        :return: `True` if data collection contains the orbit direction
        """
        defined_direction = self.orbit_direction
        if defined_direction is None or defined_direction.upper() == OrbitDirection.BOTH:
            return True
        return orbit_direction.upper() == defined_direction.upper()

    @classmethod
    def get_available_collections(cls) -> List["DataCollection"]:
        """Returns which data collections are available for configured Sentinel Hub OGC URL

        :return: List of available data collections
        """
        return list(cls)

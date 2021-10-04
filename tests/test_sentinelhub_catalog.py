"""
Tests for the module with Catalog API interface
"""
import datetime as dt

import dateutil.tz
import pytest

from sentinelhub import SentinelHubCatalog, DataCollection, BBox, CRS, Geometry
from sentinelhub.sentinelhub_catalog import CatalogSearchIterator


TEST_BBOX = BBox([46.16, -16.15, 46.51, -15.58], CRS.WGS84)

pytestmark = pytest.mark.sh_integration


@pytest.fixture(name='catalog')
def catalog_fixture(config):
    return SentinelHubCatalog(config=config)


@pytest.mark.parametrize('data_collection', [
    DataCollection.SENTINEL2_L2A,
    DataCollection.LANDSAT_TM_L1,
    DataCollection.SENTINEL3_OLCI
])
def test_info_with_different_deployments(config, data_collection):
    """ Test if basic interaction works with different data collections on different deployments
    """
    config.sh_base_url = data_collection.service_url or config.sh_base_url
    catalog = SentinelHubCatalog(config=config)
    info = catalog.get_info()

    assert isinstance(info, dict)
    assert all(link['href'].startswith(config.sh_base_url) for link in info['links'])


def test_conformance(catalog):
    """ Test conformance endpoint
    """
    conformance = catalog.get_conformance()
    assert isinstance(conformance, dict)


def test_get_collections(catalog):
    """ Tests collections endpoint
    """
    collections = catalog.get_collections()

    assert isinstance(collections, list)
    assert len(collections) >= 3


@pytest.mark.parametrize('collection_input', [
    'sentinel-2-l1c',
    DataCollection.SENTINEL1_IW
])
def test_get_collection(catalog, collection_input):
    """ Test endpoint for a single collection info
    """
    collection_info = catalog.get_collection(collection_input)
    assert isinstance(collection_info, dict)


def test_get_feature(catalog):
    """ Test endpoint for a single feature info
    """
    feature_id = 'S2B_MSIL2A_20200318T120639_N0214_R080_T24FWD_20200318T135608'
    feature_info = catalog.get_feature(DataCollection.SENTINEL2_L2A, feature_id)

    assert isinstance(feature_info, dict)
    assert feature_info['id'] == feature_id


def test_search_bbox(catalog):
    """ Tests search with bounding box
    """
    time_interval = '2021-01-01T00:00:00', '2021-01-15T00:00:10'
    cloud_cover_interval = 10, 50

    search_iterator = catalog.search(
        collection=DataCollection.SENTINEL2_L1C,
        time=time_interval,
        bbox=TEST_BBOX.transform(CRS.POP_WEB),
        query={
            'eo:cloud_cover': {
                'gt': cloud_cover_interval[0],
                'lt': cloud_cover_interval[1]
            }
        },
        limit=2
    )

    assert isinstance(search_iterator, CatalogSearchIterator)

    for _ in range(3):
        result = next(search_iterator)
        assert isinstance(result, dict)
        assert time_interval[0] <= result['properties']['datetime'] <= time_interval[1]
        assert cloud_cover_interval[0] <= result['properties']['eo:cloud_cover'] <= cloud_cover_interval[1]


def test_search_geometry_and_iterator_methods(catalog):
    """ Tests search with a geometry and test methods of CatalogSearchIterator
    """
    search_geometry = Geometry(TEST_BBOX.geometry, crs=TEST_BBOX.crs)

    search_iterator = catalog.search(
        collection=DataCollection.SENTINEL2_L1C,
        time=('2021-01-01', '2021-01-5'),
        geometry=search_geometry,
        query={
            'eo:cloud_cover': {
                'lt': 40
            }
        }
    )
    results = list(search_iterator)

    assert len(results) == 1
    assert search_iterator.get_timestamps() == [dt.datetime(2021, 1, 3, 7, 14, 7, tzinfo=dateutil.tz.tzutc())]
    assert search_iterator.get_ids() == ['S2A_MSIL1C_20210103T071211_N0209_R020_T38LPH_20210103T083459']

    geometries = search_iterator.get_geometries()
    assert len(geometries) == 1
    assert isinstance(geometries[0], Geometry)
    assert geometries[0].geometry.intersects(search_geometry.geometry)


@pytest.mark.parametrize('data_collection, feature_id', [
    (DataCollection.SENTINEL2_L1C, 'S2A_MSIL1C_20210113T071211_N0209_R020_T38LPH_20210113T075941'),
    ('sentinel-2-l1c', 'S2A_MSIL1C_20210113T071211_N0209_R020_T38LPH_20210113T075941'),
    (DataCollection.SENTINEL2_L2A, 'S2A_MSIL2A_20210113T071211_N0214_R020_T38LPH_20210113T083244'),
    (DataCollection.SENTINEL1_IW, 'S1A_IW_GRDH_1SDV_20210113T022710_20210113T022735_036113_043BC9_2981'),
    (DataCollection.LANDSAT_OT_L1, 'LC08_L1TP_160071_20210113_20210308_02_T1'),
    (DataCollection.LANDSAT_OT_L2, 'LC08_L2SP_160071_20210113_20210308_02_T1'),
    (DataCollection.MODIS, 'MCD43A4.006/22/10/2021014/MCD43A4.A2021014.h22v10.006.2021025214119'),
    (DataCollection.SENTINEL3_OLCI,
     'S3A_OL_1_EFR____20210114T063914_20210114T064214_20210115T095516_0179_067_191_3240_LN1_O_NT_002.SEN3'),
    (DataCollection.SENTINEL3_SLSTR,
     'S3A_SL_1_RBT____20210114T190809_20210114T191109_20210116T001252_0179_067_198_5760_LN2_O_NT_004.SEN3'),
    (DataCollection.SENTINEL5P, 'S5P_NRTI_L2__AER_AI_20210114T100354_20210114T100854_16869_01_010400_20210114T104450')
])
def test_search_for_data_collection(config, data_collection, feature_id):
    """ Tests search functionality for each data collection to confirm compatibility between DataCollection parameters
    and Catalog API
    """
    collection_base_url = data_collection.service_url if isinstance(data_collection, DataCollection) else None
    config.sh_base_url = collection_base_url or config.sh_base_url
    catalog = SentinelHubCatalog(config=config)

    search_iterator = catalog.search(
        collection=data_collection,
        time=('2021-01-01T00:00:00', '2021-01-15T00:00:10'),
        bbox=TEST_BBOX,
        limit=1,
    )
    result = next(search_iterator)
    assert isinstance(result, dict)
    assert result['id'] == feature_id

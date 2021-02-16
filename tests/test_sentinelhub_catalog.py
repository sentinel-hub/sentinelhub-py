"""
Tests for the module with Catalog API interface
"""
import datetime as dt

import dateutil.tz
import pytest

from sentinelhub import SentinelHubCatalog, DataCollection, BBox, CRS, Geometry
from sentinelhub.sentinelhub_catalog import CatalogSearchIterator


TEST_BBOX = BBox([46.16, -16.15, 46.51, -15.58], CRS.WGS84)


@pytest.fixture(name='catalog')
def catalog_fixture(config):
    return SentinelHubCatalog(config=config)


@pytest.mark.parametrize('data_collection', [
    DataCollection.SENTINEL2_L2A,
    DataCollection.LANDSAT8,
    DataCollection.SENTINEL3_OLCI
])
def test_info_with_different_deployments(config, data_collection):
    """ Test if basic interaction works with different data collections on different deployments
    """
    catalog = SentinelHubCatalog(base_url=data_collection.service_url, config=config)
    info = catalog.get_info()

    assert isinstance(info, dict)
    expected_url = data_collection.service_url or config.sh_base_url
    assert all(link['href'].startswith(expected_url) for link in info['links'])


def test_conformance(catalog):
    """ Test conformance endpoint
    """
    conformance = catalog.get_conformance()
    assert isinstance(conformance, dict)


def test_collections(catalog):
    """ Tests collections endpoint
    """
    collections = catalog.get_collections()

    assert isinstance(collections, list)
    assert len(collections) >= 3


@pytest.mark.parametrize('collection_input', [
    'sentinel-2-l1c',
    DataCollection.SENTINEL1_IW
])
def test_collection_info(catalog, collection_input):
    """ Test endpoint for a single collection info
    """
    collection_info = catalog.get_collection_info(collection_input)
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


@pytest.mark.parametrize('data_collection', [
    DataCollection.SENTINEL2_L1C,
    DataCollection.SENTINEL2_L2A,
    DataCollection.SENTINEL1_EW,
    DataCollection.LANDSAT8,
    DataCollection.MODIS,
    DataCollection.SENTINEL3_OLCI,
    DataCollection.SENTINEL3_SLSTR,
    DataCollection.SENTINEL5P
])
def test_search_for_data_collection(config, data_collection):
    """ Tests search functionality for each data collection to confirm compatibility between DataCollection parameters
    and Catalog API
    """
    catalog = SentinelHubCatalog(base_url=data_collection.service_url, config=config)

    search_iterator = catalog.search(
        collection=data_collection,
        time=('2021-01-01T00:00:00', '2021-01-15T00:00:10'),
        bbox=TEST_BBOX,
        limit=1,
    )
    result = next(search_iterator)
    assert isinstance(result, dict)

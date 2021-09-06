"""
Unit tests for data_collections module
"""
import pytest

from sentinelhub import DataCollection
from sentinelhub.constants import ServiceUrl
from sentinelhub.data_collections import DataCollectionDefinition, _RENAMED_COLLECTIONS
from sentinelhub.exceptions import SHDeprecationWarning


def test_repr():
    definition = DataCollection.SENTINEL1_IW.value
    representation = repr(definition)

    assert isinstance(representation, str)
    assert representation.count('\n') >= 5


def test_derive():
    definition = DataCollectionDefinition(api_id='X', wfs_id='Y')
    derived_definition = definition.derive(wfs_id='Z')

    assert derived_definition.api_id == 'X'
    assert derived_definition.wfs_id == 'Z'
    assert derived_definition.collection_type is None


def test_compare():
    def1 = DataCollectionDefinition(api_id='X', _name='A')
    def2 = DataCollectionDefinition(api_id='X', _name='B')

    assert def1 == def2


def test_define():
    for _ in range(3):
        data_collection = DataCollection.define(
            'NEW',
            api_id='X',
            sensor_type='Sensor',
            bands=('B01',),
            is_timeless=True
        )

    assert data_collection == DataCollection.NEW

    with pytest.raises(ValueError):
        DataCollection.define(
            'NEW_NEW',
            api_id='X',
            sensor_type='Sensor',
            bands=('B01',),
            is_timeless=True
        )

    with pytest.raises(ValueError):
        DataCollection.define(
            'NEW',
            api_id='Y'
        )


def test_define_from():
    bands = ['B01', 'XYZ']
    for _ in range(3):
        data_collection = DataCollection.define_from(
            DataCollection.SENTINEL5P,
            'NEW_5P',
            api_id='X',
            bands=bands
        )

    assert data_collection == DataCollection.NEW_5P
    assert data_collection.api_id == 'X'
    assert data_collection.wfs_id == DataCollection.SENTINEL5P.wfs_id
    assert data_collection.bands == tuple(bands)


def test_define_byoc_and_batch():
    byoc_id = '0000d273-7e89-4f00-971e-9024f89a0000'
    byoc = DataCollection.define_byoc(byoc_id, name='MY_BYOC')
    batch = DataCollection.define_batch(byoc_id, name='MY_BATCH')

    assert byoc == DataCollection.MY_BYOC
    assert batch == DataCollection.MY_BATCH

    for data_collection in [byoc, batch]:
        assert data_collection.api_id.endswith(byoc_id)
        assert data_collection.collection_id == byoc_id


def test_attributes():
    data_collection = DataCollection.SENTINEL3_OLCI

    for attr_name in ['api_id', 'catalog_id', 'wfs_id', 'service_url', 'bands', 'sensor_type']:
        value = getattr(data_collection, attr_name)
        assert value is not None
        assert value == getattr(data_collection.value, attr_name)

    data_collection = DataCollection.define('EMPTY')

    for attr_name in ['api_id', 'catalog_id', 'wfs_id', 'bands']:
        with pytest.raises(ValueError):
            getattr(data_collection, attr_name)

    assert data_collection.service_url is None


def test_sentinel1_checks():
    assert DataCollection.SENTINEL1_IW.is_sentinel1
    assert not DataCollection.SENTINEL2_L1C.is_sentinel1

    assert DataCollection.SENTINEL1_IW_ASC.contains_orbit_direction('ascending')
    assert not DataCollection.SENTINEL1_IW_DES.contains_orbit_direction('ascending')

    assert DataCollection.SENTINEL2_L2A.contains_orbit_direction('descending')


def test_renamed_collections():
    """ Makes sure that for all renamed collections new names are correctly assigned and deprecation warning is raised.
    """
    for old_name, new_name in _RENAMED_COLLECTIONS.items():
        with pytest.warns(SHDeprecationWarning):
            collection = getattr(DataCollection, old_name)

        assert collection.name == new_name


def test_get_available_collections(config):
    collections = DataCollection.get_available_collections()
    assert helper_check_collection_list(collections)

    config.sh_base_url = ServiceUrl.EOCLOUD
    eocloud_collections = DataCollection.get_available_collections(config=config)
    assert helper_check_collection_list(eocloud_collections)
    assert eocloud_collections != collections


def helper_check_collection_list(collection_list):
    is_list = isinstance(collection_list, list)
    contains_collections = all(isinstance(data_collection, DataCollection) for data_collection in collection_list)
    return is_list and contains_collections


@pytest.fixture(name='ray')
def ray_fixture():
    """ Ensures that the ray server will stop even if test fails
    """
    ray = pytest.importorskip('ray')
    ray.init(log_to_driver=False)

    yield ray
    ray.shutdown()


def test_transfer_with_ray(ray):
    """ This tests makes sure that the process of transferring a custom DataCollection object to a Ray worker and back
    works correctly.
    """
    collection = DataCollection.SENTINEL2_L1C.define_from('MY_NEW_COLLECTION', api_id='xxx')

    collection_future = ray.remote(lambda x: x).remote(collection)
    transferred_collection = ray.get(collection_future)

    assert collection is transferred_collection

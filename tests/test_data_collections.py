"""
Unit tests for data_collections module
"""
import unittest

import ray

from sentinelhub import DataCollection, TestSentinelHub, SHConfig
from sentinelhub.constants import ServiceUrl
from sentinelhub.data_collections import DataCollectionDefinition
from sentinelhub.exceptions import SHDeprecationWarning


class TestDataCollectionDefinition(TestSentinelHub):
    def test_repr(self):
        definition = DataCollection.SENTINEL1_IW.value
        representation = repr(definition)

        self.assertTrue(isinstance(representation, str))
        self.assertTrue(representation.count('\n') >= 5)

    def test_derive(self):
        definition = DataCollectionDefinition(
            api_id='X',
            wfs_id='Y'
        )
        derived_definition = definition.derive(wfs_id='Z')

        self.assertEqual(derived_definition.api_id, 'X')
        self.assertEqual(derived_definition.wfs_id, 'Z')
        self.assertEqual(derived_definition.collection_type, None)

    def test_compare(self):
        def1 = DataCollectionDefinition(api_id='X', _name='A')
        def2 = DataCollectionDefinition(api_id='X', _name='B')

        self.assertEqual(def1, def2)


class TestDataCollection(TestSentinelHub):

    def test_define(self):
        for _ in range(3):
            data_collection = DataCollection.define(
                'NEW',
                api_id='X',
                sensor_type='Sensor',
                bands=('B01',),
                is_timeless=True
            )

        self.assertEqual(data_collection, DataCollection.NEW)

        with self.assertRaises(ValueError):
            DataCollection.define(
                'NEW_NEW',
                api_id='X',
                sensor_type='Sensor',
                bands=('B01',),
                is_timeless=True
            )

        with self.assertRaises(ValueError):
            DataCollection.define(
                'NEW',
                api_id='Y'
            )

    def test_define_from(self):
        bands = ['B01', 'XYZ']
        for _ in range(3):
            data_collection = DataCollection.define_from(
                DataCollection.SENTINEL5P,
                'NEW_5P',
                api_id='X',
                bands=bands
            )

        self.assertEqual(data_collection, DataCollection.NEW_5P)
        self.assertEqual(data_collection.api_id, 'X')
        self.assertEqual(data_collection.wfs_id, DataCollection.SENTINEL5P.wfs_id)
        self.assertEqual(data_collection.bands, tuple(bands))

    def test_define_byoc_and_batch(self):
        byoc_id = '0000d273-7e89-4f00-971e-9024f89a0000'
        byoc = DataCollection.define_byoc(byoc_id, name=f'MY_BYOC')
        batch = DataCollection.define_batch(byoc_id, name='MY_BATCH')

        self.assertEqual(byoc, DataCollection.MY_BYOC)
        self.assertEqual(batch, DataCollection.MY_BATCH)

        for ds in [byoc, batch]:
            self.assertTrue(ds.api_id.endswith(byoc_id))
            self.assertEqual(ds.collection_id, byoc_id)

        with self.assertWarns(SHDeprecationWarning):
            byoc2 = DataCollection(byoc_id.replace('0', '1'))

        self.assertTrue(byoc, byoc2)

    def test_attributes(self):
        ds = DataCollection.SENTINEL3_OLCI

        for attr_name in ['api_id', 'catalog_id', 'wfs_id', 'service_url', 'bands', 'sensor_type']:
            value = getattr(ds, attr_name)
            self.assertNotEqual(value, None)
            self.assertEqual(value, getattr(ds.value, attr_name))

        ds = DataCollection.define('EMPTY')

        for attr_name in ['api_id', 'catalog_id', 'wfs_id', 'bands']:
            with self.assertRaises(ValueError):
                getattr(ds, attr_name)

        self.assertEqual(ds.service_url, None)

    def test_sentine1_checks(self):
        self.assertTrue(DataCollection.SENTINEL1_IW.is_sentinel1)
        self.assertFalse(DataCollection.SENTINEL2_L1C.is_sentinel1)

        self.assertTrue(DataCollection.SENTINEL1_IW_ASC.contains_orbit_direction('ascending'))
        self.assertFalse(DataCollection.SENTINEL1_IW_DES.contains_orbit_direction('ascending'))

        self.assertTrue(DataCollection.SENTINEL2_L2A.contains_orbit_direction('descending'))

    def test_get_available_collections(self):
        collections = DataCollection.get_available_collections()
        self._check_collection_list(collections)

        config = SHConfig()
        config.sh_base_url = ServiceUrl.EOCLOUD
        eocloud_collections = DataCollection.get_available_collections(config=config)
        self._check_collection_list(eocloud_collections)
        self.assertNotEqual(eocloud_collections, collections)

    def _check_collection_list(self, collection_list):
        self.assertTrue(isinstance(collection_list, list))
        self.assertTrue(all(isinstance(data_collection, DataCollection) for data_collection in collection_list))


def test_data_collection_transfer_with_ray():
    """ This tests makes sure that the process of transferring a custom DataCollection object to a Ray worker and back
    works correctly.
    """
    ray.init(log_to_driver=False)

    collection = DataCollection.SENTINEL2_L1C.define_from('MY_NEW_COLLECTION', api_id='xxx')

    collection_future = ray.remote(lambda x: x).remote(collection)
    transferred_collection = ray.get(collection_future)

    assert collection is transferred_collection

    ray.shutdown()


if __name__ == '__main__':
    unittest.main()

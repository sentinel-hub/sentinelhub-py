"""
Unit tests for data_sources module
"""
import unittest

from sentinelhub import DataSource, TestSentinelHub, SHConfig
from sentinelhub.constants import ServiceUrl
from sentinelhub.data_sources import DataSourceDefinition
from sentinelhub.exceptions import SHDeprecationWarning


class TestDataSourceDefinition(TestSentinelHub):
    def test_repr(self):
        definition = DataSource.SENTINEL1_IW.value
        representation = repr(definition)

        self.assertTrue(isinstance(representation, str))
        self.assertTrue(representation.count('\n') >= 5)

    def test_derive(self):
        definition = DataSourceDefinition(
            api_id='X',
            wfs_id='Y'
        )
        derived_definition = definition.derive(wfs_id='Z')

        self.assertEqual(derived_definition.api_id, 'X')
        self.assertEqual(derived_definition.wfs_id, 'Z')
        self.assertEqual(derived_definition.source, None)


class TestDataSource(TestSentinelHub):

    def test_define(self):
        for _ in range(3):
            datasource = DataSource.define(
                'NEW',
                api_id='X',
                sensor_type='Sensor',
                bands=('B01',),
                is_timeless=True
            )

        self.assertEqual(datasource, DataSource.NEW)

        with self.assertRaises(ValueError):
            DataSource.define(
                'NEW_NEW',
                api_id='X',
                sensor_type='Sensor',
                bands=('B01',),
                is_timeless=True
            )

        with self.assertRaises(ValueError):
            DataSource.define(
                'NEW',
                api_id='Y'
            )

    def test_define_from(self):
        bands = ['B01', 'XYZ']
        for _ in range(3):
            datasource = DataSource.define_from(
                DataSource.SENTINEL5P,
                'NEW_5P',
                api_id='X',
                bands=bands
            )

        self.assertEqual(datasource, DataSource.NEW_5P)
        self.assertEqual(datasource.api_id, 'X')
        self.assertEqual(datasource.wfs_id, DataSource.SENTINEL5P.wfs_id)
        self.assertEqual(datasource.bands, tuple(bands))

    def test_define_byoc_and_batch(self):
        byoc_id = '0000d273-7e89-4f00-971e-9024f89a0000'
        byoc = DataSource.define_byoc(byoc_id, name=f'MY_BYOC')
        batch = DataSource.define_batch(byoc_id, name='MY_BATCH')

        self.assertEqual(byoc, DataSource.MY_BYOC)
        self.assertEqual(batch, DataSource.MY_BATCH)

        for ds in [byoc, batch]:
            self.assertTrue(ds.api_id.endswith(byoc_id))
            self.assertEqual(ds.collection_id, byoc_id)

        with self.assertWarns(SHDeprecationWarning):
            byoc2 = DataSource(byoc_id.replace('0', '1'))

        self.assertTrue(byoc, byoc2)

    def test_attributes(self):
        ds = DataSource.SENTINEL3_OLCI

        for attr_name in ['api_id', 'wfs_id', 'service_url', 'bands', 'sensor_type']:
            value = getattr(ds, attr_name)
            self.assertNotEqual(value, None)
            self.assertEqual(value, getattr(ds.value, attr_name))

        ds = DataSource.define('EMPTY')

        for attr_name in ['api_id', 'wfs_id', 'bands']:
            with self.assertRaises(ValueError):
                getattr(ds, attr_name)

        self.assertEqual(ds.service_url, None)

    def test_sentine1_checks(self):
        self.assertTrue(DataSource.SENTINEL1_IW.is_sentinel1)
        self.assertFalse(DataSource.SENTINEL2_L1C.is_sentinel1)

        self.assertTrue(DataSource.SENTINEL1_IW_ASC.contains_orbit_direction('ascending'))
        self.assertFalse(DataSource.SENTINEL1_IW_DES.contains_orbit_direction('ascending'))

        self.assertTrue(DataSource.SENTINEL2_L2A.contains_orbit_direction('descending'))

    def test_get_available_sources(self):
        sources = DataSource.get_available_sources()
        self._check_source_list(sources)

        config = SHConfig()
        config.sh_base_url = ServiceUrl.EOCLOUD
        eocloud_sources = DataSource.get_available_sources(config=config)
        self._check_source_list(eocloud_sources)
        self.assertNotEqual(eocloud_sources, sources)

    def _check_source_list(self, source_list):
        self.assertTrue(isinstance(source_list, list))
        self.assertTrue(all(isinstance(data_source, DataSource) for data_source in source_list))


if __name__ == '__main__':
    unittest.main()

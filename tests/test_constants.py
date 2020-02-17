import unittest

from sentinelhub import CRS, MimeType, TestSentinelHub
from sentinelhub.constants import RequestType, DataSource


class TestCRS(TestSentinelHub):
    def test_utm(self):
        known_values = (
            (13, 46, '32633'),
            (13, 0, '32633'),
            (13, -45, '32733'),
            (13, 0, '32633'),
            (13, -0.0001, '32733'),
            (13, -46, '32733')
        )
        for known_val in known_values:
            lng, lat, epsg = known_val
            with self.subTest(msg=epsg):
                crs = CRS.get_utm_from_wgs84(lng, lat)
                self.assertEqual(epsg, crs.value,
                                 msg="Expected {}, got {} for lng={},lat={}".format(epsg, crs.value, str(lng),
                                                                                    str(lat)))

    def test_ogc_string(self):
        crs_values = (
            (CRS.POP_WEB, 'EPSG:3857'),
            (CRS.WGS84, 'EPSG:4326'),
            (CRS.UTM_33N, 'EPSG:32633'),
            (CRS.UTM_33S, 'EPSG:32733')
        )
        for crs, epsg in crs_values:
            with self.subTest(msg=epsg):
                ogc_str = CRS.ogc_string(crs)
                self.assertEqual(epsg, ogc_str, msg="Expected {}, got {}".format(epsg, ogc_str))

    def test_repr(self):
        crs_values = (
            (CRS.POP_WEB, "CRS('3857')"),
            (CRS.WGS84, "CRS('4326')"),
            (CRS.UTM_33N, "CRS('32633')"),
            (CRS.UTM_33S, "CRS('32733')"),
            (CRS('3857'), "CRS('3857')"),
            (CRS('4326'), "CRS('4326')"),
            (CRS('32633'), "CRS('32633')"),
            (CRS('32733'), "CRS('32733')"),
        )
        for crs, crs_repr in crs_values:
            with self.subTest(msg=crs_repr):
                self.assertEqual(crs_repr, repr(crs), msg="Expected {}, got {}".format(crs_repr, repr(crs)))

    def test_has_value(self):
        for crs in CRS:
            self.assertTrue(CRS.has_value(crs.value), msg="Expected support for CRS {}".format(crs.value))

    def test_custom_crs(self):
        for incorrect_value in ['string', -1, 999, None]:
            with self.assertRaises(ValueError):
                CRS(incorrect_value)

        for correct_value in [3035, 'EPSG:3035', 10000]:
            CRS(CRS(correct_value))

            new_enum_value = str(correct_value).lower().strip('epsg: ')
            self.assertTrue(CRS.has_value(new_enum_value))


class TestMimeType(TestSentinelHub):

    def test_extension_and_from_string(self):
        extension_pairs = (
            ('tif', MimeType.TIFF),
            ('jpeg', MimeType.JPG),
            ('h5', MimeType.HDF),
            ('hdf5', MimeType.HDF)
        )
        for ext, mime_type in extension_pairs:
            parsed_mime_type = MimeType.from_string(ext)
            self.assertEqual(mime_type, parsed_mime_type)

        for mime_type in MimeType:
            if not mime_type.is_tiff_format():
                self.assertEqual(MimeType.from_string(mime_type.extension), mime_type)

        with self.assertRaises(ValueError):
            MimeType.from_string('unknown ext')

    def test_has_value(self):
        self.assertTrue(MimeType.has_value('tiff;depth=32f'))
        self.assertFalse(MimeType.has_value('unknown value'))

    def test_is_image_format(self):
        for ext in ['tif', 'tiff', 'jpg', 'jpeg', 'png', 'jp2']:
            mime_type = MimeType.from_string(ext)
            self.assertTrue(MimeType.is_image_format(mime_type),
                            msg="Expected MIME type {} to be an image format".format(mime_type.value))

    def test_get_string(self):
        type_string_pairs = (
            (MimeType.PNG, 'image/png'),
            (MimeType.JPG, 'image/jpeg'),
            (MimeType.TIFF, 'image/tiff'),
            (MimeType.TIFF_d32f, 'image/tiff;depth=32f'),
            (MimeType.JSON, 'application/json'),
            (MimeType.CSV, 'text/csv'),
            (MimeType.ZIP, 'application/zip'),
            (MimeType.HDF, 'application/x-hdf'),
            (MimeType.XML, 'text/xml'),
            (MimeType.TXT, 'text/plain'),
            (MimeType.TAR, 'application/x-tar')
        )
        for img_type, img_str in type_string_pairs:
            res = MimeType.get_string(img_type)
            self.assertEqual(img_str, res, msg="Expected {}, got {}".format(img_str, res))

    def test_get_sample_type(self):
        self.assertEqual(MimeType.TIFF_d16.get_sample_type(), 'INT16')

        with self.assertRaises(ValueError):
            MimeType.TXT.get_sample_type()

    def test_get_expected_max_value(self):
        self.assertEqual(MimeType.TIFF_d32f.get_expected_max_value(), 1.0)

        with self.assertRaises(ValueError):
            MimeType.TAR.get_expected_max_value()


class TestRequestType(TestSentinelHub):
    def test_request_type(self):
        with self.assertRaises(ValueError):
            RequestType('post')

        with self.assertRaises(ValueError):
            RequestType('get')

        try:
            RequestType('POST')
            RequestType('GET')
        except BaseException:
            self.fail("Couldn't instantiate enum")


class TestDataSource(TestSentinelHub):
    def test_adding_custom_datasource(self):
        collectionid_datasourcename_wfsid = (
            ('0000d273-7e89-4f00-971e-9025f89a0000', 'BYOC_0000d273-7e89-4f00-971e-9025f89a0000',
             'DSS10-0000d273-7e89-4f00-971e-9025f89a0000'),
        )
        for collection_id, data_source_name, wfs_id in collectionid_datasourcename_wfsid:
            datasource = DataSource(collection_id)

            wfsid_tested = DataSource.get_wfs_typename(datasource)
            self.assertEqual(data_source_name, datasource.name, msg="Expected {}, got {}".
                             format(data_source_name, datasource.name))
            self.assertEqual(wfs_id, wfsid_tested, msg="Expected {}, got {}".format(wfs_id, wfsid_tested))

            self.assertTrue(datasource in DataSource.get_available_sources(), msg='Datasource should be in the list'
                                                                                   'of all datasources')
            self.assertTrue(datasource in DataSource.get_custom_sources(), msg='Datasource should be in the list'
                                                                               'of custom datasources')


if __name__ == '__main__':
    unittest.main()

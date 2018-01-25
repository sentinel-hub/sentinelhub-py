import unittest
from tests_all import TestSentinelHub

from sentinelhub.constants import CRS, MimeType, RequestType


class TestCRS(TestSentinelHub):
    def test_utm(self):
        known_values = (
            (46, 13, '32633'),
            (0, 13, '32633'),
            (-45, 13, '32733'),
            (0, 13, '32633'),
            (-0.0001, 13, '32733'),
            (-46, 13, '32733')
        )
        for known_val in known_values:
            lat, lng, epsg = known_val
            with self.subTest(msg=epsg):
                crs = CRS.get_utm_from_wgs84(lat, lng)
                self.assertEqual(epsg, crs.value,
                                 msg="Expected {}, got {} for lat={},lng={}".format(epsg, crs.value, str(lat),
                                                                                    str(lng)))

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

    def test_has_value(self):
        for crs in CRS:
            self.assertTrue(CRS.has_value(crs.value), msg="Expected support for CRS {}".format(crs.value))


class TestMimeType(TestSentinelHub):
    def test_canonical_extension(self):
        extension_pairs = (
            ('tiff', 'tiff'),
            ('tif', 'tiff'),
            ('jpg', 'jpg'),
            ('jpeg', 'jpg'),
            ('png', 'png'),
            ('jp2', 'jp2'),
            ('txt', 'txt'),
            ('h5', 'hdf'),
            ('hdf', 'hdf'),
            ('hdf5', 'hdf')
        )
        for ext, canon in extension_pairs:
            res = MimeType.canonical_extension(ext)
            self.assertEqual(canon, res, msg="Expected {}, got {}".format(canon, res))

    def test_is_image_format(self):
        image_format_extensions = (
            'tif', 'tiff', 'jpg', 'jpeg', 'tif', 'tiff', 'png', 'jpg'
        )
        for ext in image_format_extensions:
            mime_type = MimeType(MimeType.canonical_extension(ext))
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
            (MimeType.XML, 'application/xml'),
            (MimeType.TXT, 'text/plain')
        )
        for img_type, img_str in type_string_pairs:
            res = MimeType.get_string(img_type)
            self.assertEqual(img_str, res, msg="Expected {}, got {}".format(img_str, res))


class TestRequestType(TestSentinelHub):
    def test_request_type(self):
        with self.assertRaises(ValueError):
            RequestType('post')

        with self.assertRaises(ValueError):
            RequestType('get')

        try:
            RequestType('POST')
            RequestType('GET')
        except:
            self.fail("Couldn't instantiate enum")


if __name__ == '__main__':
    unittest.main()

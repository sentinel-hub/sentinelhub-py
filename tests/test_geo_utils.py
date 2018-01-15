import unittest

from sentinelhub import geo_utils
from sentinelhub.constants import CRS


class TestGeo(unittest.TestCase):
    def test_wgs84_to_utm33N(self):
        x, y = geo_utils.wgs84_to_utm(44.1440478, 15.525078, CRS.UTM_33N)
        expected_x = 541995.694062
        expected_y = 4888006.132887
        self.assertAlmostEqual(x, expected_x, delta=1E-2, msg="Expected {}, got {}".format(str(expected_x), str(x)))
        self.assertAlmostEqual(y, expected_y, delta=1E-2, msg="Expected {}, got {}".format(str(expected_y), str(y)))

    def test_utm33N_wgs84(self):
        lat, lon = geo_utils.utm_to_wgs84(541995.694062, 4888006.132887, CRS.UTM_33N)
        expected_lat = 44.1440478
        expected_lon = 15.525078
        self.assertAlmostEqual(lat, expected_lat, delta=1E-6, msg="Expected {}, got {}".format(str(expected_lat), str(lat)))
        self.assertAlmostEqual(lon, expected_lon, delta=1E-6, msg="Expected {}, got {}".format(str(expected_lon), str(lon)))

    def test_get_utm_epsg_from_latlon(self):
        lat, lon = 44.14, 15.52
        expected_crs = CRS.UTM_33N
        crs = geo_utils.get_utm_epsg_from_latlon(lat, lon)
        self.assertEqual(crs, expected_crs, msg="Expected {}, got {}".format(expected_crs, crs))

    def test_transform_point(self):
        with self.assertRaises(NotImplementedError):
            geo_utils.transform_point(None, None, None)

    def test_transform_bbox(self):
        with self.assertRaises(NotImplementedError):
            geo_utils.transform_bbox(None, None)


if __name__ == '__main__':
    unittest.main()

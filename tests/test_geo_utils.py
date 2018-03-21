import unittest
from tests_all import TestSentinelHub

from sentinelhub import geo_utils
from sentinelhub.constants import CRS
from sentinelhub.common import BBox


class TestGeo(TestSentinelHub):
    def test_wgs84_to_utm33N(self):
        x, y = geo_utils.wgs84_to_utm(15.525078, 44.1440478, CRS.UTM_33N)
        expected_x = 541995.694062
        expected_y = 4888006.132887
        self.assertAlmostEqual(x, expected_x, delta=1E-2, msg="Expected {}, got {}".format(str(expected_x), str(x)))
        self.assertAlmostEqual(y, expected_y, delta=1E-2, msg="Expected {}, got {}".format(str(expected_y), str(y)))

    def test_utm33N_wgs84(self):
        lng, lat = geo_utils.to_wgs84(541995.694062, 4888006.132887, CRS.UTM_33N)
        expected_lng = 15.525078
        expected_lat = 44.1440478
        self.assertAlmostEqual(lng, expected_lng, delta=1E-6, msg="Expected {}, got {}".format(str(expected_lng),
                                                                                               str(lng)))
        self.assertAlmostEqual(lat, expected_lat, delta=1E-6, msg="Expected {}, got {}".format(str(expected_lat),
                                                                                               str(lat)))

    def test_get_utm_epsg_from_lnglat(self):
        lng, lat = 15.52, 44.14
        expected_crs = CRS.UTM_33N
        crs = geo_utils.get_utm_crs(lng, lat)
        self.assertEqual(crs, expected_crs, msg="Expected {}, got {}".format(expected_crs, crs))

    def test_bbox_to_resolution(self):
        bbox = BBox(((111.644, 8.655), (111.7, 8.688)), CRS.WGS84)
        resx, resy = geo_utils.bbox_to_resolution(bbox, 512, 512)
        expected_resx = 12.02
        expected_resy = 7.15
        self.assertAlmostEqual(resx, expected_resx, delta=1E-2,
                               msg="Expected resx {}, got {}".format(str(expected_resx), str(resx)))
        self.assertAlmostEqual(resy, expected_resy, delta=1E-2,
                               msg="Expected resy {}, got {}".format(str(expected_resy), str(resy)))

    def test_get_image_dimensions(self):
        bbox = BBox(((111.644, 8.655), (111.7, 8.688)), CRS.WGS84)
        width = geo_utils.get_image_dimension(bbox, height=715)
        height = geo_utils.get_image_dimension(bbox, width=1202)
        expected_width = 1203
        expected_height = 715
        self.assertEqual(width, expected_width, msg="Expected width {}, got {}".format(expected_width, width))
        self.assertEqual(height, expected_height, msg="Expected height {}, got {}".format(expected_height, height))


if __name__ == '__main__':
    unittest.main()

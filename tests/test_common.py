import unittest
from tests_all import TestSentinelHub

import shapely.geometry

from sentinelhub import BBox, CRS


class TestBBox(TestSentinelHub):
    def test_bbox_no_crs(self):
        with self.assertRaises(TypeError):
            BBox('46,13,47,20')

    def test_bbox_from_string(self):
        bbox_str = '46.07, 13.23, 46.24, 13.57'
        bbox = BBox(bbox_str, CRS.WGS84)
        self.assertEqual(bbox.get_lower_left(), (46.07, 13.23))
        self.assertEqual(bbox.get_upper_right(), (46.24, 13.57))
        self.assertEqual(bbox.get_crs(), CRS.WGS84)

    def test_bbox_from_bad_string(self):
        with self.subTest(msg="Too few coordinates"):
            bbox_str = '46.07, 13.23, 46.24'
            with self.assertRaises(ValueError):
                BBox(bbox_str, CRS.WGS84)

        with self.subTest(msg="Invalid string"):
            bbox_str = '46N,13E,45N,12E'
            with self.assertRaises(ValueError):
                BBox(bbox_str, CRS.WGS84)

    def test_bbox_from_flat_list(self):
        for bbox_lst in [[46.07, 13.23, 46.24, 13.57], [46.24, 13.23, 46.07, 13.57],
                         [46.07, 13.57, 46.24, 13.23], [46.24, 13.57, 46.07, 13.23]]:
            with self.subTest(msg="bbox={}".format(bbox_lst)):
                bbox = BBox(bbox_lst, CRS.WGS84)
                self.assertEqual(bbox.get_lower_left(), (46.07, 13.23))
                self.assertEqual(bbox.get_upper_right(), (46.24, 13.57))
                self.assertEqual(bbox.get_crs(), CRS.WGS84)

    def test_bbox_from_nested_list(self):
        bbox_lst = [[-46.07, -13.23], [46.24, 13.57]]
        bbox = BBox(bbox_lst, CRS.WGS84)
        self.assertEqual(bbox.get_upper_right(), (46.24, 13.57))
        self.assertEqual(bbox.get_lower_left(), (-46.07, -13.23))
        self.assertEqual(bbox.get_crs(), CRS.WGS84)

    def test_bbox_from_flat_tuple(self):
        bbox_tup = 46.07, 13.23, 46.24, 13.57
        bbox = BBox(bbox_tup, CRS.WGS84)
        self.assertEqual(bbox.get_upper_right(), (46.24, 13.57))
        self.assertEqual(bbox.get_lower_left(), (46.07, 13.23))
        self.assertEqual(bbox.get_crs(), CRS.WGS84)

    def test_bbox_from_nested_tuple(self):
        bbox_tup = (46.07, 13.23), (46.24, 13.57)
        bbox = BBox(bbox_tup, CRS.WGS84)
        self.assertEqual(bbox.get_upper_right(), (46.24, 13.57))
        self.assertEqual(bbox.get_lower_left(), (46.07, 13.23))
        self.assertEqual(bbox.get_crs(), CRS.WGS84)

    def test_bbox_from_list_tuple_combo(self):
        bbox_list = [(46.07, 13.23), (46.24, 13.57)]
        bbox = BBox(bbox_list, CRS.WGS84)
        self.assertEqual(bbox.get_upper_right(), (46.24, 13.57))
        self.assertEqual(bbox.get_lower_left(), (46.07, 13.23))
        self.assertEqual(bbox.get_crs(), CRS.WGS84)

    def test_bbox_from_dict(self):
        bbox_dict = {'min_x': 46.07, 'min_y': 13.23, 'max_x': 46.24, 'max_y': 13.57}
        bbox = BBox(bbox_dict, CRS.WGS84)
        self.assertEqual(bbox.get_upper_right(), (46.24, 13.57))
        self.assertEqual(bbox.get_lower_left(), (46.07, 13.23))
        self.assertEqual(bbox.get_crs(), CRS.WGS84)

    def test_bbox_from_bad_dict(self):
        bbox_dict = {'x1': 46.07, 'y1': 13.23, 'x2': 46.24, 'y2': 13.57}
        with self.assertRaises(KeyError):
            BBox(bbox_dict, CRS.WGS84)

    def test_bbox_from_bbox(self):
        bbox_dict = {'min_x': 46.07, 'min_y': 13.23, 'max_x': 46.24, 'max_y': 13.57}
        bbox_fst = BBox(bbox_dict, CRS.WGS84)
        bbox = BBox(bbox_fst, CRS.WGS84)

        self.assertEqual(bbox.get_upper_right(), (46.24, 13.57))
        self.assertEqual(bbox.get_lower_left(), (46.07, 13.23))
        self.assertEqual(bbox.get_crs(), CRS.WGS84)

    def test_bbox_to_str(self):
        x1, y1, x2, y2 = 45.0, 12.0, 47.0, 14.0
        crs = CRS.WGS84
        expect_str = "{},{},{},{}".format(x1, y1, x2, y2)
        bbox = BBox(((x1, y1), (x2, y2)), crs)
        self.assertEqual(str(bbox), expect_str,
                         msg="String representations not matching: expected {}, got {}".format(
                             expect_str, str(bbox)
                         ))

    def test_bbox_iter(self):
        bbox_lst = [46.07, 13.23, 46.24, 13.57]
        bbox = BBox(bbox_lst, CRS.WGS84)
        bbox_iter = [coord for coord in bbox]
        self.assertEqual(bbox_iter, bbox_lst,
                         msg="Expected {}, got {}".format(bbox_lst, bbox_iter))

    def test_bbox_eq(self):
        bbox1 = BBox([46.07, 13.23, 46.24, 13.57], CRS.WGS84)
        bbox2 = BBox(((46.24, 13.57), (46.07, 13.23)), 4326)
        bbox3 = BBox([46.07, 13.23, 46.24, 13.57], CRS.POP_WEB)
        bbox4 = BBox([46.07, 13.23, 46.24, 13.58], CRS.WGS84)
        self.assertEqual(bbox1, bbox2, "Bounding boxes {} and {} should be the same".format(repr(bbox1), repr(bbox2)))
        self.assertNotEqual(bbox1, bbox3, "Bounding boxes {} and {} should not be the same".format(repr(bbox1),
                                                                                                   repr(bbox3)))
        self.assertNotEqual(bbox1, bbox4, "Bounding boxes {} and {} should not be the same".format(repr(bbox1),
                                                                                                   repr(bbox4)))

    def test_geometry(self):
        bbox = BBox([46.07, 13.23, 46.24, 13.57], CRS.WGS84)

        self.assertTrue(isinstance(bbox.get_geojson(), dict),
                        "Expected dictionary, got type {}".format(type(bbox.get_geometry())))
        self.assertTrue(isinstance(bbox.get_geometry(), shapely.geometry.polygon.Polygon),
                        "Expected type {}, got type {}".format(shapely.geometry.polygon.Polygon,
                                                               type(bbox.get_geometry())))


if __name__ == '__main__':
    unittest.main()

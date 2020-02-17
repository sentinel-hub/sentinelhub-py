import unittest
import os
import itertools

import shapely.geometry

from sentinelhub import BBoxSplitter, OsmSplitter, TileSplitter, CustomGridSplitter, BBox, read_data, CRS, \
    DataSource, TestSentinelHub, UtmGridSplitter, UtmZoneSplitter


class TestAreaSplitters(TestSentinelHub):

    class SplitterTestCase:

        def __init__(self, name, splitter, bbox_len):
            self.name = name
            self.splitter = splitter
            self.bbox_len = bbox_len

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        geojson = read_data(os.path.join(cls.INPUT_FOLDER, 'cies_islands.json'))
        cls.area = shapely.geometry.shape(geojson)

        bbox_grid = [BBox((x / 10, y / 100, (x + 1) / 10, (y + 1) / 100), CRS.WGS84)
                     for x, y in itertools.product(range(-90, -87), range(4200, 4250))]

        cls.test_cases = [
            cls.SplitterTestCase('BBoxSplitter',
                                 BBoxSplitter([cls.area], CRS.WGS84, 5, reduce_bbox_sizes=True), bbox_len=19),
            cls.SplitterTestCase('OsmSplitter',
                                 OsmSplitter([cls.area], CRS.WGS84, 15, reduce_bbox_sizes=True), bbox_len=24),
            cls.SplitterTestCase('TileSplitter',
                                 TileSplitter([cls.area], CRS.WGS84, ('2017-10-01', '2018-03-01'), tile_split_shape=40,
                                              data_source=DataSource.SENTINEL2_L1C, reduce_bbox_sizes=True),
                                 bbox_len=13),
            cls.SplitterTestCase('CustomGridSplitter',
                                 CustomGridSplitter([cls.area], CRS.WGS84, bbox_grid, bbox_split_shape=(3, 4),
                                                    reduce_bbox_sizes=False),
                                 bbox_len=41),
            cls.SplitterTestCase('UTMGridSplitter',
                                 UtmGridSplitter([cls.area], CRS.WGS84, bbox_size=(1200, 1200)), bbox_len=16),
            cls.SplitterTestCase('UTMZoneSplitter',
                                 UtmZoneSplitter([cls.area], CRS.WGS84, bbox_size=(1000, 1000)), bbox_len=19)
        ]

    def test_return_type(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                return_lists = [
                    (test_case.splitter.get_bbox_list(buffer=0.2), BBox),
                    (test_case.splitter.get_info_list(), dict),
                    (test_case.splitter.get_geometry_list(), (shapely.geometry.Polygon, shapely.geometry.MultiPolygon))
                ]

                for return_list, item_type in return_lists:
                    self.assertTrue(isinstance(return_list, list), "Expected a list")
                    self.assertEqual(len(return_list), test_case.bbox_len,
                                     "Expected a list of length {}, got length {}".format(test_case.bbox_len,
                                                                                          len(return_list)))
                    for return_item in return_list:
                        self.assertTrue(isinstance(return_item, item_type),
                                        "Expected items of type {}, got {}".format(item_type, type(return_item)))


if __name__ == '__main__':
    unittest.main()

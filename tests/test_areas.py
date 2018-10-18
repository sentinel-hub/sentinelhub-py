import unittest
import os.path
from shapely.geometry import shape

from tests_all import TestSentinelHub

from sentinelhub import BBoxSplitter, OsmSplitter, TileSplitter, read_data, CRS, DataSource


class TestOgc(TestSentinelHub):

    class SplitterTestCase:

        def __init__(self, name, splitter, bbox_len):
            self.name = name
            self.splitter = splitter
            self.bbox_len = bbox_len

    @classmethod
    def setUpClass(cls):
        geojson = read_data(os.path.join(cls.INPUT_FOLDER, 'cies_islands.json'))
        cls.area = shape(geojson)

        cls.test_cases = [
            cls.SplitterTestCase('BBoxSplitter',
                                 BBoxSplitter([cls.area], CRS.WGS84, 5, reduce_bbox_sizes=True), bbox_len=19),
            cls.SplitterTestCase('OsmSplitter',
                                 OsmSplitter([cls.area], CRS.WGS84, 15, reduce_bbox_sizes=True), bbox_len=24),
            cls.SplitterTestCase('TileSplitter',
                                 TileSplitter([cls.area], CRS.WGS84, ('2017-10-01', '2018-03-01'), tile_split_shape=40,
                                              data_source=DataSource.SENTINEL2_L1C, reduce_bbox_sizes=True),
                                 bbox_len=13)
        ]

    def test_return_type(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                bbox_list = test_case.splitter.get_bbox_list()
                info_list = test_case.splitter.get_info_list()
                self.assertTrue(isinstance(bbox_list, list), "Expected a list")
                self.assertTrue(isinstance(info_list, list), "Expected a list")
                self.assertEqual(len(bbox_list), test_case.bbox_len,
                                 "Expected a list of length {}, got length {}".format(test_case.bbox_len,
                                                                                      len(bbox_list)))
                self.assertEqual(len(info_list), test_case.bbox_len,
                                 "Expected a list of length {}, got length {}".format(test_case.bbox_len,
                                                                                      len(info_list)))


if __name__ == '__main__':
    unittest.main()

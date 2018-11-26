import unittest

from sentinelhub import get_tile_info, get_area_dates, BBox, CRS, TestSentinelHub


class TestOpensearch(TestSentinelHub):
    def test_get_tile_info(self):
        tile_info = get_tile_info('T30SVH', '2015-11-29', aws_index=1)
        self.assertTrue(isinstance(tile_info, dict), msg="Expected a dict, got {}".format(type(tile_info)))

    def test_get_area_dates(self):
        bbox = BBox([1059111.463919402, 4732980.791418114, 1061557.4488245277, 4735426.776323237], crs=CRS.POP_WEB)
        dates = get_area_dates(bbox, ('2016-01-23', '2016-11-24'), maxcc=0.7)
        self.assertTrue(isinstance(dates, list), msg="Expected a list, got {}".format(type(dates)))
        self.assertEqual(len(dates), 22, "Expected a list of length 22, got length {}".format(len(dates)))


if __name__ == '__main__':
    unittest.main()

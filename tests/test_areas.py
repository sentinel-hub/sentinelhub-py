import unittest
import os.path
from shapely.geometry import shape

from tests_all import TestSentinelHub

from sentinelhub.areas import BBoxSplitter
from sentinelhub.io_utils import read_data
from sentinelhub.constants import CRS


class TestOgc(TestSentinelHub):

    @classmethod
    def setUpClass(cls):
        geojson = read_data(os.path.join(cls.INPUT_FOLDER, 'cies_islands.json'))
        cls.area = shape(geojson)

        cls.splitter = BBoxSplitter([cls.area], CRS.WGS84, 5)
        cls.bbox_list = cls.splitter.get_bbox_list()

    def test_return_type(self):
        data_len = 19
        self.assertTrue(isinstance(self.bbox_list, list), "Expected a list")
        self.assertEqual(len(self.bbox_list), data_len,
                         "Expected a list of length {}, got length {}".format(data_len, len(self.bbox_list)))

    '''    
    def test_stats(self):
        delta = 1e-1 if np.issubdtype(self.data[0].dtype, np.integer) else 1e-4

        min_val = np.amin(self.data[0])
        min_exp = 0
        self.assertAlmostEqual(min_exp, min_val, delta=delta, msg="Expected min {}, got {}".format(min_exp, min_val))
        max_val = np.amax(self.data[0])
        max_exp = 255
        self.assertAlmostEqual(max_exp, max_val, delta=delta, msg="Expected max {}, got {}".format(max_exp, max_val))
        mean_val = np.mean(self.data[0])
        mean_exp = 150.9248
        self.assertAlmostEqual(mean_exp, mean_val, delta=delta,
                               msg="Expected mean {}, got {}".format(mean_exp, mean_val))
        median_val = np.median(self.data[0])
        media_exp = 255
        self.assertAlmostEqual(media_exp, median_val, delta=delta,
                               msg="Expected median {}, got {}".format(media_exp, median_val))
    '''

if __name__ == '__main__':
    unittest.main()

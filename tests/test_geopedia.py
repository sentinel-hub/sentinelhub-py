import unittest
import numpy as np

from tests_all import TestSentinelHub

from sentinelhub import GeopediaWmsRequest, CRS, MimeType, BBox


class TestOgc(TestSentinelHub):

    @classmethod
    def setUpClass(cls):
        bbox = BBox(bbox=[(524358.0140363087, 6964349.630376049),
                          (534141.9536568124, 6974133.5699965535)], crs=CRS.POP_WEB)

        gpd_request = GeopediaWmsRequest(layer='ttl1917', theme='ml_aws', bbox=bbox, width=50, height=50,
                                         image_format=MimeType.PNG)

        cls.data = gpd_request.get_data()

    def test_return_type(self):
        data_len = 1
        self.assertTrue(isinstance(self.data, list), "Expected a list")
        self.assertEqual(len(self.data), data_len,
                         "Expected a list of length {}, got length {}".format(data_len, len(self.data)))

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


if __name__ == '__main__':
    unittest.main()

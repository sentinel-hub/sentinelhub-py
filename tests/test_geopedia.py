import unittest
import numpy as np

from tests_all import TestSentinelHub

from sentinelhub.data_request import GeopediaWmsRequest, GeopediaImageRequest
from sentinelhub.geopedia import GeopediaFeatureIterator, GeopediaImageService
from sentinelhub.constants import CRS, MimeType
from sentinelhub.common import BBox


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


class TestImageService(TestSentinelHub):

    @classmethod
    def setUpClass(cls):
        cls.bbox = BBox(bbox=[(1618936.259080, 5789913.800031),
                              (1622796.329008, 5787085.629985)],
                        crs=CRS.POP_WEB)

    def test_present_image_format(self):
        gpd_request = GeopediaImageRequest('393', self.bbox, 'Photo',
                                           image_format=MimeType.JPG)
        gpd_service = GeopediaImageService()
        download_list = gpd_service.get_request(gpd_request)

        self.assertTrue(len(download_list) > 0)

    def test_absent_image_format(self):
        gpd_request = GeopediaImageRequest('393', self.bbox, 'Photo',
                                           image_format=MimeType.PNG)
        gpd_service = GeopediaImageService()
        download_list = gpd_service.get_request(gpd_request)

        self.assertEqual(0, len(download_list))

class TestFeatureIterator(TestSentinelHub):

    @classmethod
    def setUpClass(cls):
        cls.bbox = BBox(bbox=[(1618936.259080, 5789913.800031),
                              (1622796.329008, 5787085.629985)],
                        crs=CRS.POP_WEB)

    def test_return_type(self):
        gpd_iter = GeopediaFeatureIterator('393', bbox=self.bbox)
        data = list(gpd_iter)

        self.assertTrue(isinstance(data, list), "Expected a list")

    def test_item_count(self):
        gpd_iter = GeopediaFeatureIterator('393', bbox=self.bbox)
        data = list(gpd_iter)
        expected_data_len = 1

        self.assertEqual(len(data), expected_data_len)

    def test_without_bbox(self):
        gpd_iter = GeopediaFeatureIterator('393')
        data = list(gpd_iter)
        expected_data_len = 147

        self.assertEqual(len(data), expected_data_len)

if __name__ == '__main__':
    unittest.main()

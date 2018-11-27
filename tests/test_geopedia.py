import unittest
import numpy as np

from sentinelhub import GeopediaWmsRequest, GeopediaImageRequest, GeopediaFeatureIterator, CRS, MimeType, BBox,\
    TestSentinelHub


class TestGeopediaWms(TestSentinelHub):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

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
        self.test_numpy_data(np.array(self.data), exp_min=0, exp_max=255, exp_mean=150.9248, exp_median=255)


class TestGeopediaImageService(TestSentinelHub):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        bbox = BBox(bbox=[(13520759, 437326), (13522689, 438602)], crs=CRS.POP_WEB)
        cls.image_field_name = 'Masks'

        cls.gpd_request = GeopediaImageRequest(layer=1749, bbox=bbox, image_field_name=cls.image_field_name,
                                               image_format=MimeType.PNG, data_folder=cls.OUTPUT_FOLDER)
        cls.image_list = cls.gpd_request.get_data(save_data=True)

    def test_return_type(self):
        self.assertTrue(isinstance(self.image_list, list), 'Expected a list, got {}'.format(type(self.image_list)))

        expected_len = 5
        self.assertEqual(len(self.image_list), expected_len,
                         "Expected a list of length {}, got length {}".format(expected_len, len(self.image_list)))

    def test_stats(self):
        self.test_numpy_data(np.array(self.image_list), exp_min=0, exp_max=255, exp_mean=66.88769, exp_median=0)

    def test_names(self):
        filenames = self.gpd_request.get_filename_list()
        image_stats = list(self.gpd_request.get_items())[0]['properties'][self.image_field_name]

        for filename, image_stat in zip(filenames, image_stats):
            self.assertEqual(filename, image_stat['niceName'].replace(' ', '_'), 'Filenames dont match')


class TestGeopediaFeatureIterator(TestSentinelHub):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.bbox = BBox(bbox=[(2947363, 4629723), (3007595, 4669471)], crs=CRS.POP_WEB)
        cls.bbox.transform(CRS.WGS84)

    def test_item_count(self):
        gpd_iter = GeopediaFeatureIterator(1749, bbox=self.bbox)
        data = list(gpd_iter)
        minimal_data_len = 21

        self.assertTrue(len(data) >= minimal_data_len, 'Expected at least {} results, got {}'.format(minimal_data_len,
                                                                                                     len(data)))

    def test_without_bbox(self):
        gpd_iter = GeopediaFeatureIterator(1749)

        minimal_data_len = 1000

        for idx, class_item in enumerate(gpd_iter):
            self.assertTrue(isinstance(class_item, dict), 'Expected at dictionary, got {}'.format(type(class_item)))

            if idx >= minimal_data_len - 1:
                break

        self.assertEqual(gpd_iter.index, minimal_data_len, 'Expected at least {} results, '
                                                           'got {}'.format(minimal_data_len, gpd_iter.index))


if __name__ == '__main__':
    unittest.main()

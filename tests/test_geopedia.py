import unittest
import datetime

import numpy as np

from sentinelhub import GeopediaSession, GeopediaWmsRequest, GeopediaImageRequest, GeopediaFeatureIterator, \
    CRS, MimeType, BBox, TestSentinelHub, TestCaseContainer


class TestGeopediaSession(TestSentinelHub):
    # When config.json could store Geopedia credentials add some login tests

    def test_global_session(self):
        session1 = GeopediaSession(is_global=True)
        session2 = GeopediaSession(is_global=True)
        session3 = GeopediaSession(is_global=False)

        self.assertEqual(session1.session_id, session2.session_id, 'Global sessions should have the same session ID')
        self.assertNotEqual(session1.session_id, session3.session_id,
                            'Global and local sessions should not have the same session ID')

    def test_session_update(self):
        session = GeopediaSession()
        initial_session_id = session.session_id

        self.assertNotEqual(session.get_session_id(start_new=True), initial_session_id, 'Session should be updated')

    def test_session_timeout(self):
        session = GeopediaSession()
        session.SESSION_DURATION = datetime.timedelta(seconds=-1)
        initial_session_id = session.session_id

        self.assertNotEqual(session.session_id, initial_session_id, 'Session should timeout and be updated')


class TestGeopediaWms(TestSentinelHub):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        bbox = BBox(bbox=[(524358.0140363087, 6964349.630376049),
                          (534141.9536568124, 6974133.5699965535)], crs=CRS.POP_WEB)

        gpd_request = GeopediaWmsRequest(layer=1917, theme='ml_aws', bbox=bbox, width=50, height=50,
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
                                               image_format=MimeType.PNG, data_folder=cls.OUTPUT_FOLDER,
                                               gpd_session=GeopediaSession(is_global=True))
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

        bbox = BBox(bbox=[(2947363, 4629723), (3007595, 4669471)], crs=CRS.POP_WEB)
        bbox = bbox.transform(CRS.WGS84)
        query_filter1 = 'f12458==32632'
        query_filter2 = 'f12458==32635'

        cls.test_cases = [
            TestCaseContainer('All features', GeopediaFeatureIterator(1749, gpd_session=GeopediaSession()),
                              min_features=100, min_size=1609),
            TestCaseContainer('BBox filter', GeopediaFeatureIterator('1749', bbox=bbox), min_features=21),
            TestCaseContainer('Query Filter', GeopediaFeatureIterator('ttl1749', query_filter=query_filter1),
                              min_features=76),
            TestCaseContainer('Both filters - No data',
                              GeopediaFeatureIterator(1749, bbox=bbox, query_filter=query_filter1), min_features=0),
            TestCaseContainer('Both filters - Some data',
                              GeopediaFeatureIterator(1749, bbox=bbox, query_filter=query_filter2), min_features=21)
        ]

    def test_iterator(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):

                gpd_iter = test_case.request
                for idx, feature in enumerate(gpd_iter):
                    self.assertTrue(isinstance(feature, dict), 'Expected at dictionary, got {}'.format(type(feature)))

                    if idx >= test_case.min_features - 1:
                        break

                self.assertEqual(gpd_iter.index, test_case.min_features,
                                 'Expected at least {} features, got {}'.format(test_case.min_features, gpd_iter.index))

                if test_case.min_size:
                    self.assertTrue(test_case.min_size <= len(gpd_iter),
                                    'There should be at least {} features available, '
                                    'got {}'.format(test_case.min_size, gpd_iter.get_size()))


if __name__ == '__main__':
    unittest.main()

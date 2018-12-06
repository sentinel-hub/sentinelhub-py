import unittest
import ast
import os

from shapely.geometry import Polygon

from sentinelhub import CRS, DataSource
from sentinelhub import BBox, Geometry
from sentinelhub import TestCaseContainer, CustomUrlParam
from sentinelhub import FisRequest, TestSentinelHub


class TestFis(TestSentinelHub):
    class FisTestCase(TestCaseContainer):
        """
        Container for each test case of sentinelhub FIS functionalities
        """

        def __init__(self, name, request, save_data=False, **kwargs):
            super().__init__(name, request, **kwargs)

            self.save_data = save_data
            self.data = None

        def collect_data(self):
            if self.save_data:
                self.request.save_data(redownload=True, data_filter=self.data_filter)
                self.data = self.request.get_data(save_data=True, data_filter=self.data_filter)
            else:
                self.data = self.request.get_data(redownload=True, data_filter=self.data_filter)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with open(os.path.join(cls.INPUT_FOLDER, "test_fis_results.txt"), 'r') as file:
            results = [ast.literal_eval(line.strip()) for line in file]

        wgs84_bbox = BBox(bbox=[46.16, -16.15, 46.51, -15.58], crs=CRS.WGS84)
        pop_web_bbox = BBox(bbox=(1292344.0, 5195920.0, 1310615.0, 5214191.0), crs=CRS.POP_WEB)
        wgs84_geometry = Geometry(polygon=Polygon([(-5.13, 48),
                                                   (-5.23, 48.09),
                                                   (-5.13, 48.17),
                                                   (-5.03, 48.08),
                                                   (-5.13, 48)]),
                                  crs=CRS.WGS84)

        pop_web_geometry = Geometry(polygon=Polygon([(1292344.0, 5205055.5),
                                                     (1301479.5, 5195920.0),
                                                     (1310615.0, 5205055.5),
                                                     (1301479.5, 5214191.0),
                                                     (1292344.0, 5205055.5)]),
                                    crs=CRS.POP_WEB)

        cls.test_cases = [
            cls.FisTestCase('wgs84_bbox',
                            FisRequest(layer='TRUE-COLOR-S2-L1C',
                                       geometry_list=[wgs84_bbox],
                                       time=('2017-1-1', '2017-2-1'),
                                       resolution="100m",
                                       style="REFLECTANCE",
                                       bins=5),
                            raw_result=results[0],
                            result_length=1),
            cls.FisTestCase('pop_web_bbox',
                            FisRequest(layer='BANDS-S2-L1C',
                                       geometry_list=[pop_web_bbox],
                                       time='2017-1-1',
                                       resolution="200m",
                                       style="INDEX"),
                            raw_result=results[1],
                            result_length=1),
            cls.FisTestCase('landsat',
                            FisRequest(data_source=DataSource.LANDSAT8,
                                       layer='BANDS-L8',
                                       geometry_list=[wgs84_bbox, pop_web_bbox],
                                       time=('2017-1-1', '2017-2-1'),
                                       resolution="100m",
                                       style="SENSOR",
                                       bins=32),
                            raw_result=results[2],
                            result_length=2),
            cls.FisTestCase('longer_list',
                            FisRequest(layer='TRUE-COLOR-S2-L1C',
                                       geometry_list=[pop_web_bbox, wgs84_geometry, pop_web_geometry],
                                       time=('2017-1-1', '2017-2-1'),
                                       resolution="100m",
                                       maxcc=20),
                            raw_result=results[3],
                            result_length=3),
            cls.FisTestCase('custom_url_params',
                            FisRequest(layer='TRUE-COLOR-S2-L1C',
                                       geometry_list=[wgs84_geometry, pop_web_bbox],
                                       time=('2017-1-1', '2017-2-1'),
                                       resolution="400m",
                                       custom_url_params={
                                                          CustomUrlParam.ATMFILTER: "ATMCOR",
                                                          CustomUrlParam.DOWNSAMPLING: "BICUBIC",
                                                          CustomUrlParam.UPSAMPLING: "BICUBIC"
                                                         }
                                       ),
                            raw_result=results[4],
                            result_length=2
                            )
        ]

        for test_case in cls.test_cases:
            test_case.collect_data()

    def test_return_type(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                self.assertTrue(isinstance(test_case.request.get_data()[0], dict),
                                "Expected dict, got {}".format(type(test_case.request.get_data()[0])))

    def test_return_length(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                self.assertEqual(len(test_case.request.get_data()), test_case.result_length,
                                 "Expected a list of length {}, got length {}".format(
                                                        test_case.result_length,
                                                        len(test_case.request.get_data())))

    def test_raw_result(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                self.assertEqual(test_case.request.get_data(), test_case.raw_result,
                                 )


if __name__ == '__main__':
    unittest.main()

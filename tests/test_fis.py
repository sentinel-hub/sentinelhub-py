import unittest
import ast
import os

from shapely.geometry import Polygon

from sentinelhub import CRS, DataCollection
from sentinelhub import BBox, Geometry
from sentinelhub import TestCaseContainer, CustomUrlParam
from sentinelhub import FisRequest, TestSentinelHub
from sentinelhub import HistogramType


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

        bbox = BBox([14.00, 45.00, 14.03, 45.03], crs=CRS.WGS84)
        geometry1 = Geometry(Polygon([(465888.877326859, 5079639.436138632),
                                      (465885.3413983975, 5079641.524618266),
                                      (465882.9542217017, 5079647.166043535),
                                      (465888.8780175466, 5079668.703676634),
                                      (465888.877326859, 5079639.436138632)]),
                             CRS(32633))
        geometry2 = Geometry('POLYGON((-5.13 48, -5.23 48.09, -5.13 48.17, -5.03 48.08, -5.13 48))', CRS.WGS84)

        cls.test_cases = [
            cls.FisTestCase('geometry',
                            FisRequest(data_collection=DataCollection.SENTINEL2_L1C,
                                       layer='TRUE-COLOR-S2-L1C',
                                       geometry_list=[geometry1],
                                       time=('2017-1-1', '2017-2-1'),
                                       resolution="50m",
                                       histogram_type=HistogramType.STREAMING,
                                       bins=5),
                            raw_result=results[0],
                            result_length=1),
            cls.FisTestCase('bbox',
                            FisRequest(data_collection=DataCollection.SENTINEL2_L1C,
                                       layer='BANDS-S2-L1C',
                                       geometry_list=[bbox],
                                       time='2017-1-1',
                                       resolution="50m",
                                       maxcc=0.2,
                                       custom_url_params={
                                           CustomUrlParam.ATMFILTER: "ATMCOR",
                                           CustomUrlParam.DOWNSAMPLING: "BICUBIC",
                                           CustomUrlParam.UPSAMPLING: "BICUBIC"}
                                       ),
                            raw_result=results[1],
                            result_length=1),
            cls.FisTestCase('list',
                            FisRequest(data_collection=DataCollection.LANDSAT8,
                                       layer='BANDS-L8',
                                       geometry_list=[bbox, geometry1],
                                       time=('2017-1-1', '2017-1-10'),
                                       resolution="100m",
                                       bins=32, data_folder=cls.OUTPUT_FOLDER),
                            raw_result=results[2], result_length=2,
                            save_data=True),
            cls.FisTestCase('Polygon in WGS84',
                            FisRequest(data_collection=DataCollection.SENTINEL2_L1C,
                                       layer='TRUE-COLOR-S2-L1C',
                                       geometry_list=[geometry2],
                                       time=('2017-10-1', '2017-10-2'),
                                       resolution="60m",
                                       bins=11, histogram_type=HistogramType.EQUALFREQUENCY),
                            raw_result=results[3], result_length=1),
        ]

        for test_case in cls.test_cases:
            test_case.collect_data()

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
                self.assertEqual(test_case.request.get_data(), test_case.raw_result)


if __name__ == '__main__':
    unittest.main()

import unittest
import datetime
import numpy as np

from tests_all import TestSentinelHub

from sentinelhub.ogc import OgcImageService
from sentinelhub.data_request import WmsRequest, WcsRequest, OgcRequest
from sentinelhub.constants import CRS, MimeType, CustomUrlParam, ServiceType, DataSource
from sentinelhub.common import BBox


class TestOgc(TestSentinelHub):

    class OgcTestCase:
        """
        Container for each test case of sentinelhub OGC functionalities
        """
        def __init__(self, name, request, result_len, img_min=None, img_max=None, img_mean=None, img_median=None,
                     tile_num=None, url_check=None, date_check=None, save_data=False):
            self.name = name
            self.request = request
            self.result_len = result_len
            self.img_min = img_min
            self.img_max = img_max
            self.img_mean = img_mean
            self.img_median = img_median
            self.tile_num = tile_num
            self.url_check = url_check
            self.date_check = date_check
            self.save_data = save_data

            self.data = None

        def collect_data(self):
            if self.save_data:
                self.request.save_data(redownload=True)
                self.data = self.request.get_data(save_data=True)
            else:
                self.data = self.request.get_data(redownload=True)

    @classmethod
    def setUpClass(cls):
        wgs84_bbox = BBox(bbox=(-5.23, 48.0, -5.03, 48.17), crs=CRS.WGS84)
        wgs84_bbox_2 = BBox(bbox=(21.3, 64.0, 22.0, 64.5), crs=CRS.WGS84)
        wgs84_bbox_3 = BBox(bbox=(-72.0, -70.4, -71.8, -70.2), crs=CRS.WGS84)
        pop_web_bbox = BBox(bbox=(1292344.0, 5195920.0, 1310615.0, 5214191.0), crs=CRS.POP_WEB)
        img_width = 100
        img_height = 100
        resx='53m'
        resy='78m'
        expected_date = datetime.datetime.strptime('2017-10-07T11:20:58', '%Y-%m-%dT%H:%M:%S')

        cls.test_cases = [
            cls.OgcTestCase('generalWmsTest',
                            OgcRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.TIFF_d32f, bbox=wgs84_bbox,
                                       layer='ALL_BANDS', maxcc=0.5, size_x=img_width, size_y=img_height,
                                       time=('2017-10-01', '2018-01-01'), instance_id=cls.INSTANCE_ID,
                                       service_type=ServiceType.WMS, time_difference=datetime.timedelta(days=10)),
                            result_len=3, img_min=0.0, img_max=0.4544, img_mean=0.1038, img_median=0.0945, tile_num=7,
                            date_check=expected_date, save_data=True),
            cls.OgcTestCase('generalWcsTest',
                            OgcRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.TIFF_d32f, bbox=wgs84_bbox,
                                       layer='ALL_BANDS', maxcc=0.6, size_x=resx, size_y=resy,
                                       time=('2017-10-01', '2018-01-01'), instance_id=cls.INSTANCE_ID,
                                       service_type=ServiceType.WCS, time_difference=datetime.timedelta(days=5)),
                            result_len=5, img_min=0.0002, img_max=0.5266, img_mean=0.1038, img_median=0.0948,
                            tile_num=9, date_check=expected_date, save_data=True),

            # CustomUrlParam tests:
            cls.OgcTestCase('customUrlAtmcorQualitySampling',
                            WmsRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.PNG, layer='TRUE_COLOR',
                                       width=img_width, bbox=wgs84_bbox, instance_id=cls.INSTANCE_ID,
                                       time=('2017-10-01', '2017-10-02'),
                                       custom_url_params={CustomUrlParam.SHOWLOGO: True,
                                                          CustomUrlParam.ATMFILTER: 'ATMCOR',
                                                          CustomUrlParam.QUALITY: 100,
                                                          CustomUrlParam.DOWNSAMPLING: 'BICUBIC',
                                                          CustomUrlParam.UPSAMPLING: 'BICUBIC'}),
                            result_len=1, img_min=11, img_max=255, img_mean=193.796, img_median=206, tile_num=2),
            cls.OgcTestCase('customUrlPreview',
                            WmsRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.PNG, layer='TRUE_COLOR',
                                       height=img_height, bbox=wgs84_bbox, instance_id=cls.INSTANCE_ID,
                                       time=('2017-10-01', '2017-10-02'),
                                       custom_url_params={CustomUrlParam.SHOWLOGO: True,
                                                          CustomUrlParam.PREVIEW: 2}),
                            result_len=1, img_min=27, img_max=253, img_mean=176.732, img_median=177, tile_num=2),
            cls.OgcTestCase('customUrlEvalscripturl',
                            WcsRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.PNG, layer='TRUE_COLOR',
                                       resx=resx, resy=resy, bbox=pop_web_bbox, instance_id=cls.INSTANCE_ID,
                                       time=('2017-10-01', '2017-10-02'),
                                       custom_url_params={CustomUrlParam.SHOWLOGO: True,
                                                          CustomUrlParam.EVALSCRIPTURL:
                                                          'https://raw.githubusercontent.com/sentinel-hub/'
                                                          'customScripts/master/sentinel-2/false_color_infrared/'
                                                          'script.js'}),
                            result_len=1, img_min=41, img_max=255, img_mean=230.568, img_median=255, tile_num=3),
            cls.OgcTestCase('customUrlEvalscript',
                            WcsRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.PNG, layer='TRUE_COLOR',
                                       resx=resx, resy=resy, bbox=wgs84_bbox, instance_id=cls.INSTANCE_ID,
                                       time=('2017-10-01', '2017-10-02'),
                                       custom_url_params={CustomUrlParam.SHOWLOGO: True,
                                                          CustomUrlParam.EVALSCRIPT: 'return [B10,B8A, B03 ]'}),
                            result_len=1, img_min=0, img_max=235, img_mean=48.539, img_median=55, tile_num=2),
            cls.OgcTestCase('TransparentFalseLogo',
                            WmsRequest(data_folder=cls.OUTPUT_FOLDER, image_format=MimeType.PNG, layer='TRUE_COLOR',
                                       width=img_width, height=img_height, bbox=pop_web_bbox,
                                       instance_id=cls.INSTANCE_ID, time=('2017-10-01', '2017-10-02'),
                                       custom_url_params={CustomUrlParam.SHOWLOGO: False,
                                                          CustomUrlParam.TRANSPARENT: True}),
                            result_len=1, img_min=48, img_max=255, img_mean=230.9804, img_median=242.0, tile_num=3),

            # DataSource tests:
            cls.OgcTestCase('S2 L1C Test',
                            WmsRequest(data_source=DataSource.SENTINEL2_L1C, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-S2-L1C',
                                       width=img_width, height=img_height, bbox=wgs84_bbox,
                                       instance_id=cls.INSTANCE_ID, time=('2017-10-01', '2017-10-02')),
                            result_len=1, img_min=0.00089, img_max=0.6284, img_mean=0.2373, img_median=0.2477,
                            tile_num=2),
            cls.OgcTestCase('S2 L2A Test',
                            WmsRequest(data_source=DataSource.SENTINEL2_L2A, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-S2-L2A',
                                       width=img_width, height=img_height, bbox=wgs84_bbox,
                                       instance_id=cls.INSTANCE_ID, time=('2017-10-01', '2017-10-02')),
                            result_len=1, img_min=0.0, img_max=1.6720, img_mean=0.34747, img_median=0.32640,
                            tile_num=2),
            cls.OgcTestCase('L8 Test',
                            WmsRequest(data_source=DataSource.LANDSAT8, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-L8',
                                       width=img_width, height=img_height, bbox=wgs84_bbox,
                                       instance_id=cls.INSTANCE_ID, time=('2017-10-05', '2017-10-10'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=1, img_min=0.0011, img_max=285.72415, img_mean=52.06075, img_median=0.5192,
                            tile_num=2),
            cls.OgcTestCase('DEM Test',
                            WmsRequest(data_source=DataSource.DEM, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='DEM',
                                       width=img_width, height=img_height, bbox=wgs84_bbox,
                                       instance_id=cls.INSTANCE_ID),
                            result_len=1, img_min=-108.0, img_max=-18.0, img_mean=-72.1819, img_median=-72.0),
            cls.OgcTestCase('MODIS Test',
                            WmsRequest(data_source=DataSource.MODIS, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-MODIS',
                                       width=img_width, height=img_height, bbox=wgs84_bbox,
                                       instance_id=cls.INSTANCE_ID, time='2017-10-01'),
                            result_len=1, img_min=0.0, img_max=3.2767, img_mean=0.136408, img_median=0.00240,
                            tile_num=1),
            cls.OgcTestCase('S1 IW Test',
                            WmsRequest(data_source=DataSource.SENTINEL1_IW, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-S1-IW',
                                       width=img_width, height=img_height, bbox=wgs84_bbox,
                                       instance_id=cls.INSTANCE_ID, time=('2017-10-01', '2017-10-02'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=1, img_min=0.0, img_max=1.0, img_mean=0.104584, img_median=0.06160, tile_num=2),
            cls.OgcTestCase('S1 EW Test',
                            WmsRequest(data_source=DataSource.SENTINEL1_EW, data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-S1-EW',
                                       width=img_width, height=img_height, bbox=wgs84_bbox_2,
                                       instance_id=cls.INSTANCE_ID, time=('2018-2-7', '2018-2-8'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=2, img_min=0.0003, img_max=1.0, img_mean=0.53118, img_median=1.0, tile_num=3),
            cls.OgcTestCase('S1 EW SH Test',
                            WmsRequest(data_source=DataSource.SENTINEL1_EW_SH,
                                       data_folder=cls.OUTPUT_FOLDER,
                                       image_format=MimeType.TIFF_d32f, layer='BANDS-S1-EW-SH',
                                       width=img_width, height=img_height, bbox=wgs84_bbox_3,
                                       custom_url_params={CustomUrlParam.SHOWLOGO: True},
                                       instance_id=cls.INSTANCE_ID, time=('2018-2-6', '2018-2-8'),
                                       time_difference=datetime.timedelta(hours=1)),
                            result_len=1, img_min=0.006987, img_max=0.83078, img_mean=0.06599, img_median=0.0140,
                            tile_num=1)
        ]

        for test_case in cls.test_cases:
            test_case.collect_data()

    def test_return_type(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                self.assertTrue(isinstance(test_case.data, list), "Expected a list")
                self.assertEqual(len(test_case.data), test_case.result_len,
                                 "Expected a list of length {}, got length {}".format(test_case.result_len,
                                                                                      len(test_case.data)))

    def test_wfs(self):
        for test_case in self.test_cases:
            with self.subTest(msg='Test case {}'.format(test_case.name)):
                tile_iter = test_case.request.get_tiles()
                tile_n = len(list(tile_iter)) if tile_iter else None
                self.assertEqual(tile_n, test_case.tile_num,
                                 "Expected {} tiles, got {}".format(test_case.tile_num, tile_n))

    def test_download_url(self):
        for test_case in self.test_cases:
            if test_case.url_check is not None:
                with self.subTest(msg='Test case {}'.format(test_case.name)):
                    download_url = self.request.get_url_list()[0]
                    self.assertTrue(test_case.url_check in download_url,
                                    "Parameter '{}' not in download url {}.".format(test_case.url_check, download_url))

    def test_get_dates(self):
        for test_case in self.test_cases:
            if test_case.date_check is not None:
                with self.subTest(msg='Test case {}'.format(test_case.name)):
                    dates = OgcImageService(instance_id=self.INSTANCE_ID).get_dates(test_case.request)
                    self.assertEqual(len(dates), test_case.result_len,
                                     msg="Expected {} dates, got {}".format(test_case.result_len, len(dates)))
                    self.assertEqual(test_case.date_check, dates[0],
                                     msg="Expected date {}, got {}".format(test_case.date_check, dates[0]))

    def test_stats(self):
        for test_case in self.test_cases:
            delta = 1e-1 if np.issubdtype(test_case.data[0].dtype, np.integer) else 1e-4

            if test_case.img_min is not None:
                min_val = np.amin(test_case.data[0])
                with self.subTest(msg='Test case {}'.format(test_case.name)):
                    self.assertAlmostEqual(test_case.img_min, min_val, delta=delta,
                                           msg="Expected min {}, got {}".format(test_case.img_min, min_val))
            if test_case.img_max is not None:
                max_val = np.amax(test_case.data[0])
                with self.subTest(msg='Test case {}'.format(test_case.name)):
                    self.assertAlmostEqual(test_case.img_max, max_val, delta=delta,
                                           msg="Expected max {}, got {}".format(test_case.img_max, max_val))
            if test_case.img_mean is not None:
                mean_val = np.mean(test_case.data[0])
                with self.subTest(msg='Test case {}'.format(test_case.name)):
                    self.assertAlmostEqual(test_case.img_mean, mean_val, delta=delta,
                                           msg="Expected mean {}, got {}".format(test_case.img_mean, mean_val))
            if test_case.img_median is not None:
                median_val = np.median(test_case.data[0])
                with self.subTest(msg='Test case {}'.format(test_case.name)):
                    self.assertAlmostEqual(test_case.img_median, median_val, delta=delta,
                                           msg="Expected median {}, got {}".format(test_case.img_median, median_val))


if __name__ == '__main__':
    unittest.main()

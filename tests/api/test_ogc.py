from __future__ import annotations

import datetime
from dataclasses import dataclass

import numpy as np
import pytest
from numpy.testing import assert_array_equal

from sentinelhub import (
    CRS,
    BBox,
    CustomUrlParam,
    DataCollection,
    DownloadFailedException,
    MimeType,
    ResamplingType,
    ServiceType,
    WcsRequest,
    WmsRequest,
)
from sentinelhub.api.ogc import OgcImageService, OgcRequest
from sentinelhub.testing_utils import assert_statistics_match

pytestmark = pytest.mark.sh_integration


@dataclass
class OgcTestCase:
    name: str
    constructor: type[OgcRequest] | type[WcsRequest] | type[WmsRequest]
    kwargs: dict
    result_len: int
    img_min: float
    img_max: float
    img_mean: float
    img_median: float
    img_std: float = 1
    tile_num: int | None = None
    data_filter: list[int] | None = None
    date_check: datetime.datetime | None = None
    save_data: bool = False

    def initialize_request(self, output_folder: str) -> OgcRequest:
        return self.constructor(**self.kwargs, data_folder=output_folder)

    def collect_data(self, request: OgcRequest) -> list:
        if self.save_data:
            request.save_data(redownload=True, data_filter=self.data_filter)
            return request.get_data(save_data=True, data_filter=self.data_filter)
        return request.get_data(redownload=True, data_filter=self.data_filter)


wgs84_bbox = BBox(bbox=(-5.23, 48.0, -5.03, 48.17), crs=CRS.WGS84)
wgs84_bbox_2 = BBox(bbox=(21.3, 64.0, 22.0, 64.5), crs=CRS.WGS84)
wgs84_bbox_3 = BBox(bbox=(-72.0, -70.4, -71.8, -70.2), crs=CRS.WGS84)
wgs84_bbox_4 = BBox(bbox=(-72.0, -66.4, -71.8, -66.2), crs=CRS.WGS84)
pop_web_bbox = BBox(bbox=(1292344.0, 5195920.0, 1310615.0, 5214191.0), crs=CRS.POP_WEB)
img_width, img_height = 100, 100
resx, resy = "53m", "78m"


TEST_CASES = [
    OgcTestCase(
        "generalWmsTest",
        OgcRequest,
        dict(
            image_format=MimeType.TIFF,
            bbox=wgs84_bbox,
            data_collection=DataCollection.SENTINEL2_L1C,
            layer="BANDS-S2-L1C",
            maxcc=0.5,
            size_x=img_width,
            time=(datetime.date(year=2017, month=1, day=5), datetime.date(year=2017, month=12, day=16)),
            service_type=ServiceType.WMS,
            time_difference=datetime.timedelta(days=10),
            size_y=img_height,
        ),
        result_len=14,
        img_min=0.0,
        img_max=1.5964,
        img_mean=0.23954,
        img_median=0.1349,
        img_std=0.276814,
        tile_num=29,
        save_data=True,
        data_filter=[0, -2, 0],
    ),
    OgcTestCase(
        "generalWcsTest",
        OgcRequest,
        dict(
            image_format=MimeType.TIFF,
            bbox=wgs84_bbox,
            data_collection=DataCollection.SENTINEL2_L1C,
            layer="BANDS-S2-L1C",
            maxcc=0.6,
            size_x=resx,
            size_y=resy,
            time=(
                datetime.datetime(year=2017, month=10, day=7, hour=1),
                datetime.datetime(year=2017, month=12, day=11),
            ),
            service_type=ServiceType.WCS,
            time_difference=datetime.timedelta(hours=1),
        ),
        result_len=4,
        img_min=0.0002,
        img_max=1.0,
        img_mean=0.16779,
        img_median=0.1023,
        img_std=0.24020831,
        tile_num=6,
        data_filter=[0, -1],
        date_check=datetime.datetime.strptime("2017-10-07T11:20:58", "%Y-%m-%dT%H:%M:%S"),
        save_data=True,
    ),
    # CustomUrlParam tests:
    OgcTestCase(
        "customUrlLogoQualitySampling",
        WmsRequest,
        dict(
            image_format=MimeType.PNG,
            data_collection=DataCollection.SENTINEL2_L1C,
            layer="TRUE-COLOR-S2-L1C",
            width=img_width,
            bbox=wgs84_bbox,
            time=("2017-10-01", "2017-10-02"),
            custom_url_params={
                CustomUrlParam.SHOWLOGO: True,
                CustomUrlParam.QUALITY: 100,
                CustomUrlParam.DOWNSAMPLING: ResamplingType.BICUBIC,
                CustomUrlParam.UPSAMPLING: ResamplingType.BICUBIC,
            },
        ),
        result_len=1,
        img_min=29,
        img_max=255,
        img_mean=198.6254375,
        img_median=206,
        img_std=52.17095,
        tile_num=2,
        data_filter=[0, -1],
    ),
    OgcTestCase(
        "customUrlPreview",
        WmsRequest,
        dict(
            image_format=MimeType.PNG,
            data_collection=DataCollection.SENTINEL2_L1C,
            layer="TRUE-COLOR-S2-L1C",
            height=img_height,
            bbox=wgs84_bbox,
            time=("2017-10-01", "2017-10-02"),
            custom_url_params={CustomUrlParam.PREVIEW: 2},
        ),
        result_len=1,
        img_min=27,
        img_max=255,
        img_mean=195.385181,
        img_median=199,
        img_std=51.1237,
        tile_num=2,
    ),
    OgcTestCase(
        "customUrlEvalscripturl",
        WcsRequest,
        dict(
            image_format=MimeType.PNG,
            data_collection=DataCollection.SENTINEL2_L1C,
            layer="TRUE-COLOR-S2-L1C",
            resx=resx,
            resy=resy,
            bbox=pop_web_bbox,
            time=("2017-10-01", "2017-10-02"),
            custom_url_params={
                CustomUrlParam.EVALSCRIPTURL: (
                    "https://raw.githubusercontent.com/sentinel-hub/customScripts/master/"
                    "sentinel-2/false_color_infrared/script.js"
                )
            },
        ),
        result_len=1,
        img_min=46,
        img_max=255,
        img_mean=231.051154,
        img_median=255,
        img_std=45.151,
        tile_num=3,
    ),
    OgcTestCase(
        "customUrlEvalscript,Geometry",
        WcsRequest,
        dict(
            image_format=MimeType.PNG,
            data_collection=DataCollection.SENTINEL2_L1C,
            layer="TRUE-COLOR-S2-L1C",
            resx=resx,
            resy=resy,
            bbox=wgs84_bbox,
            time=("2017-10-01", "2017-10-02"),
            custom_url_params={
                CustomUrlParam.EVALSCRIPT: "return [B10, B8A, B03]",
                CustomUrlParam.GEOMETRY: "POLYGON((-5.13 48, -5.23 48.09, -5.13 48.17, -5.03 48.08, -5.13 48))",
            },
        ),
        result_len=1,
        img_min=0,
        img_max=152,
        img_mean=24.5405,
        img_median=1.0,
        img_std=35.64637,
        tile_num=2,
    ),
    OgcTestCase(
        "FalseLogo,Geometry",
        WmsRequest,
        dict(
            image_format=MimeType.PNG,
            data_collection=DataCollection.SENTINEL2_L1C,
            layer="TRUE-COLOR-S2-L1C",
            width=img_width,
            height=img_height,
            bbox=pop_web_bbox,
            time=("2017-10-01", "2017-10-02"),
            custom_url_params={
                CustomUrlParam.SHOWLOGO: False,
                CustomUrlParam.GEOMETRY: (
                    "POLYGON((1292344.0 5205055.5, 1301479.5 5195920.0, 1310615.0 5205055.5, "
                    "1301479.5 5214191.0, 1292344.0 5205055.5))"
                ),
            },
        ),
        result_len=1,
        img_min=0,
        img_max=MimeType.PNG.get_expected_max_value(),
        img_mean=119.4666,
        img_median=123,
        img_std=119.186,
        tile_num=3,
    ),
    # DataCollection tests:
    OgcTestCase(
        "S2 L1C Test",
        WmsRequest,
        dict(
            data_collection=DataCollection.SENTINEL2_L1C,
            image_format=MimeType.TIFF,
            layer="BANDS-S2-L1C",
            width=img_width,
            height=img_height,
            bbox=wgs84_bbox,
            time=("2017-10-01", "2017-10-02"),
        ),
        result_len=1,
        img_min=0.0009,
        img_max=1.0,
        img_mean=0.29175,
        img_median=0.2572,
        img_std=0.22377,
        tile_num=2,
    ),
    OgcTestCase(
        "S2 L2A Test",
        WmsRequest,
        dict(
            data_collection=DataCollection.SENTINEL2_L2A,
            image_format=MimeType.TIFF,
            layer="BANDS-S2-L2A",
            width=img_width,
            height=img_height,
            bbox=wgs84_bbox,
            time=("2017-10-01", "2017-10-02"),
        ),
        result_len=1,
        img_min=0.0,
        img_max=1.6167,
        img_mean=0.39445,
        img_median=0.3353,
        img_std=0.226799,
        tile_num=2,
    ),
    OgcTestCase(
        "L8 Test",
        WmsRequest,
        dict(
            data_collection=DataCollection.LANDSAT_OT_L1,
            image_format=MimeType.TIFF,
            layer="BANDS-L8",
            width=img_width,
            height=img_height,
            bbox=wgs84_bbox,
            time=("2017-10-05", "2017-10-10"),
            time_difference=datetime.timedelta(hours=1),
        ),
        result_len=1,
        img_min=0.0011564,
        img_max=285.16916,
        img_mean=47.74750,
        img_median=0.5325,
        img_std=105.6793,
        tile_num=2,
    ),
    OgcTestCase(
        "DEM Test",
        WmsRequest,
        dict(
            data_collection=DataCollection.DEM,
            image_format=MimeType.TIFF,
            layer="DEM",
            width=img_width,
            height=img_height,
            bbox=wgs84_bbox_2,
        ),
        result_len=1,
        img_min=0,
        img_max=74.16661,
        img_mean=1.5549539,
        img_median=1.0,
        img_std=5.3555336,
    ),
    OgcTestCase(
        "MODIS Test",
        WmsRequest,
        dict(
            data_collection=DataCollection.MODIS,
            image_format=MimeType.TIFF,
            layer="BANDS-MODIS",
            width=img_width,
            height=img_height,
            bbox=wgs84_bbox,
            time="2017-10-01",
        ),
        result_len=1,
        img_min=0.0,
        img_max=3.2767,
        img_mean=0.21029216,
        img_median=0.0027,
        img_std=0.6494421,
        tile_num=1,
    ),
    OgcTestCase(
        "S1 IW Test",
        WmsRequest,
        dict(
            data_collection=DataCollection.SENTINEL1_IW,
            image_format=MimeType.TIFF,
            layer="BANDS-S1-IW",
            width=img_width,
            height=img_height,
            bbox=wgs84_bbox,
            time=("2017-10-01", "2017-10-02"),
            time_difference=datetime.timedelta(hours=1),
        ),
        result_len=1,
        img_min=0.0,
        img_max=1.0,
        img_mean=0.3508,
        img_median=0.07607,
        img_std=0.451077,
        tile_num=2,
    ),
    OgcTestCase(
        "S1 EW Test",
        WmsRequest,
        dict(
            data_collection=DataCollection.SENTINEL1_EW,
            image_format=MimeType.TIFF,
            layer="BANDS-S1-EW",
            width=img_width,
            height=img_height,
            bbox=wgs84_bbox_2,
            time=("2018-2-7", "2018-2-8"),
            time_difference=datetime.timedelta(hours=1),
        ),
        result_len=2,
        img_min=0.0,
        img_max=1.0,
        img_mean=0.24709,
        img_median=0.0032285,
        img_std=0.426686,
        tile_num=3,
    ),
    OgcTestCase(
        "S1 EW SH Test",
        WmsRequest,
        dict(
            data_collection=DataCollection.SENTINEL1_EW_SH,
            image_format=MimeType.TIFF,
            layer="BANDS-S1-EW-SH",
            width=img_width,
            height=img_height,
            bbox=wgs84_bbox_3,
            time=("2018-2-6", "2018-2-8"),
            time_difference=datetime.timedelta(hours=1),
        ),
        result_len=1,
        img_min=0.006974,
        img_max=1.0,
        img_mean=0.5071,
        img_median=0.5276,
        img_std=0.492936,
        tile_num=1,
    ),
    OgcTestCase(
        "S1 EW ASC Test",
        WmsRequest,
        dict(
            data_collection=DataCollection.SENTINEL1_EW_ASC,
            image_format=MimeType.TIFF,
            layer="BANDS-S1-EW",
            width=img_width,
            height=img_height,
            bbox=wgs84_bbox_2,
            time=("2018-2-7", "2018-2-8"),
            time_difference=datetime.timedelta(hours=1),
        ),
        result_len=1,
        img_min=0.0,
        img_max=1.0,
        img_mean=0.34803,
        img_median=0.02383,
        img_std=0.46208,
        tile_num=2,
    ),
    OgcTestCase(
        "S1 IW DES Test",
        WmsRequest,
        dict(
            data_collection=DataCollection.SENTINEL1_IW_DES,
            image_format=MimeType.TIFF,
            layer="BANDS-S1-IW",
            width=img_width,
            height=img_height,
            bbox=wgs84_bbox,
            time=("2017-10-01", "2017-10-05"),
            time_difference=datetime.timedelta(hours=1),
        ),
        result_len=1,
        img_min=0.0,
        img_max=1.0,
        img_mean=0.3474,
        img_median=0.040695,
        img_std=0.4618,
        tile_num=1,
    ),
    OgcTestCase(
        "S3 OLCI Test",
        WmsRequest,
        dict(
            data_collection=DataCollection.SENTINEL3_OLCI,
            image_format=MimeType.TIFF,
            layer="TRUE-COLOR-S3-OLCI",
            width=img_width,
            height=img_height,
            bbox=wgs84_bbox_4,
            time=("2020-2-5", "2020-2-10"),
            time_difference=datetime.timedelta(hours=1),
        ),
        result_len=11,
        img_min=243,
        img_max=255,
        img_mean=248.80765,
        img_median=248.0,
        img_std=3.8225,
        tile_num=17,
    ),
]


@pytest.mark.parametrize("test_case", TEST_CASES)
def test_ogc(test_case: OgcTestCase, output_folder: str) -> None:
    # Run data collection
    request = test_case.initialize_request(output_folder)
    data = test_case.collect_data(request)

    assert isinstance(data, list)
    result_len = test_case.result_len if test_case.data_filter is None else len(test_case.data_filter)
    assert len(data) == result_len

    tile_iter = request.get_tiles()
    tile_n = len(list(tile_iter)) if tile_iter else None
    assert tile_n == test_case.tile_num

    if test_case.date_check is not None:
        dates = OgcImageService().get_dates(request)
        assert len(dates) == test_case.result_len
        assert test_case.date_check == dates[0]

    if test_case.data_filter is not None:
        if (test_case.data_filter[0] - test_case.data_filter[-1]) % test_case.result_len == 0:
            assert_array_equal(data[0], data[-1], err_msg="First and last output should be equal")
        else:
            assert not np.array_equal(data[0], data[-1]), "First and last output should be different"

    assert_statistics_match(
        data[0],
        exp_min=test_case.img_min,
        exp_max=test_case.img_max,
        exp_mean=test_case.img_mean,
        exp_median=test_case.img_median,
        exp_std=test_case.img_std,
        rel_delta=1e-4,
    )


def test_too_large_request() -> None:
    bbox = BBox((-5.23, 48.0, -5.03, 48.17), CRS.WGS84)
    request = WmsRequest(
        data_collection=DataCollection.SENTINEL2_L1C,
        layer="TRUE-COLOR-S2-L1C",
        height=6000,
        width=6000,
        bbox=bbox,
        time=("2017-10-01", "2017-10-02"),
    )

    with pytest.raises(DownloadFailedException):
        request.get_data()

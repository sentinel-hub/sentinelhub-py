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
    SHConfig,
    WcsRequest,
    WmsRequest,
)
from sentinelhub.api.ogc import OgcImageService, OgcRequest
from sentinelhub.testing_utils import assert_statistics_match

CDSE_UNSUPPORTED_CASES = ["L8 Test", "MODIS Test", "S2 L2A Test"]  # bugs for CDSE S2L2A case to be fixed


@dataclass
class OgcTestCase:
    name: str
    constructor: type[OgcRequest] | type[WcsRequest] | type[WmsRequest]
    kwargs: dict
    result_len: dict[str, int]
    img_stats: dict[str, dict[str, float]]
    tile_num: dict[str, int | None]
    data_filter: list[int] | None = None
    date_check: datetime.datetime | None = None
    save_data: bool = False

    def initialize_request(self, output_folder: str, config: SHConfig) -> OgcRequest:
        return self.constructor(**self.kwargs, data_folder=output_folder, config=config)

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
        result_len=dict(sh_config=14, cdse_config=14),
        img_stats=dict(
            sh_config=dict(
                min=0.0,
                max=1.5964,
                mean=0.23954,
                median=0.1349,
                std=0.276814,
            ),
            cdse_config=dict(
                min=0.0,
                max=1.515,
                mean=0.24031,
                median=0.1355,
                std=0.276942,
            ),
        ),
        tile_num=dict(sh_config=29, cdse_config=31),
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
        result_len=dict(sh_config=4, cdse_config=4),
        img_stats=dict(
            sh_config=dict(min=0.0002, max=1.0, mean=0.16779, median=0.1023, std=0.24020831),
            cdse_config=dict(min=0.0002, max=1.0, mean=0.16779, median=0.1023, std=0.24020831),
        ),
        tile_num=dict(sh_config=6, cdse_config=6),
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
        result_len=dict(sh_config=1, cdse_config=1),
        img_stats=dict(
            sh_config=dict(min=29, max=255, mean=198.6254375, median=206, std=52.17095),
            cdse_config=dict(min=22, max=255, mean=198.6723333, median=206, std=52.16517),
        ),
        tile_num=dict(sh_config=2, cdse_config=2),
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
        result_len=dict(sh_config=1, cdse_config=1),
        img_stats=dict(
            sh_config=dict(min=27, max=255, mean=195.385181, median=199, std=51.1237),
            cdse_config=dict(min=23, max=255, mean=198.920120, median=208, std=53.1669),
        ),
        tile_num=dict(sh_config=2, cdse_config=2),
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
        result_len=dict(sh_config=1, cdse_config=1),
        img_stats=dict(
            sh_config=dict(min=46, max=255, mean=231.051154, median=255, std=45.151),
            cdse_config=dict(min=33, max=255, mean=231.069772, median=255, std=44.45055),
        ),
        tile_num=dict(sh_config=3, cdse_config=3),
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
        result_len=dict(sh_config=1, cdse_config=1),
        img_stats=dict(
            sh_config=dict(min=0, max=152, mean=24.5405, median=1, std=35.64637),
            cdse_config=dict(min=0, max=255, mean=29.04125, median=1, std=46.11639),
        ),
        tile_num=dict(sh_config=2, cdse_config=2),
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
        result_len=dict(sh_config=1, cdse_config=1),
        img_stats=dict(
            sh_config=dict(min=0, max=255, mean=119.4666, median=123, std=119.186),
            cdse_config=dict(min=0, max=255, mean=119.4666, median=123, std=119.186),
        ),
        tile_num=dict(sh_config=3, cdse_config=3),
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
        result_len=dict(sh_config=1, cdse_config=1),
        img_stats=dict(
            sh_config=dict(min=0.0009, max=1.0, mean=0.29175, median=0.2572, std=0.22377),
            cdse_config=dict(min=0.0009, max=1.0, mean=0.29152, median=0.2569, std=0.22381),
        ),
        tile_num=dict(sh_config=2, cdse_config=2),
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
        result_len=dict(sh_config=1, cdse_config=1),
        img_stats=dict(
            sh_config=dict(min=0.0, max=1.6167, mean=0.39445, median=0.3353, std=0.226799),
            cdse_config=dict(min=0.0, max=1.6167, mean=0.39445, median=0.3353, std=0.226799),
        ),
        tile_num=dict(sh_config=2, cdse_config=2),
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
        result_len=dict(sh_config=1),
        img_stats=dict(
            sh_config=dict(min=0.0011564, max=285.16916, mean=47.74750, median=0.5325, std=105.6793),
        ),
        tile_num=dict(
            sh_config=2,
        ),
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
        result_len=dict(sh_config=1, cdse_config=1),
        img_stats=dict(
            sh_config=dict(min=0, max=74.16661, mean=1.5549539, median=1.0, std=5.3555336),
            cdse_config=dict(min=0, max=74.16661, mean=1.5549539, median=1.0, std=5.3555336),
        ),
        tile_num=dict(sh_config=None, cdse_config=None),
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
        result_len=dict(
            sh_config=1,
        ),
        img_stats=dict(
            sh_config=dict(
                min=0.0,
                max=3.2767,
                mean=0.21029216,
                median=0.0027,
                std=0.6494421,
            ),
        ),
        tile_num=dict(
            sh_config=1,
        ),
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
        result_len=dict(sh_config=1, cdse_config=1),
        img_stats=dict(
            sh_config=dict(min=0.0, max=1.0, mean=0.3508, median=0.07607, std=0.451077),
            cdse_config=dict(min=0.0, max=1.0, mean=0.3960, median=0.08645, std=0.456168),
        ),
        tile_num=dict(sh_config=2, cdse_config=2),
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
        result_len=dict(sh_config=2, cdse_config=2),
        img_stats=dict(
            sh_config=dict(min=0.0, max=1.0, mean=0.24709, median=0.0032285, std=0.426686),
            cdse_config=dict(min=0.0, max=1.0, mean=0.2881186, median=0.00402838, std=0.439555),
        ),
        tile_num=dict(sh_config=3, cdse_config=3),
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
        result_len=dict(sh_config=1, cdse_config=1),
        img_stats=dict(
            sh_config=dict(min=0.006974, max=1.0, mean=0.5071, median=0.5276, std=0.492936),
            cdse_config=dict(min=0.0069407, max=1.0, mean=0.50713, median=0.53206, std=0.492868),
        ),
        tile_num=dict(sh_config=1, cdse_config=1),
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
        result_len=dict(sh_config=1, cdse_config=1),
        img_stats=dict(
            sh_config=dict(min=0.0, max=1.0, mean=0.34803, median=0.02383, std=0.46208),
            cdse_config=dict(min=0.00030518, max=1.0, mean=0.398073, median=0.035227, std=0.466808),
        ),
        tile_num=dict(sh_config=2, cdse_config=2),
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
        result_len=dict(sh_config=1, cdse_config=1),
        img_stats=dict(
            sh_config=dict(min=0.0, max=1.0, mean=0.3474, median=0.040695, std=0.4618),
            cdse_config=dict(min=0.0, max=1.0, mean=0.3967, median=0.045994, std=0.467266),
        ),
        tile_num=dict(sh_config=1, cdse_config=1),
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
        result_len=dict(sh_config=11, cdse_config=11),
        img_stats=dict(
            sh_config=dict(min=243, max=255, mean=248.80765, median=248.0, std=3.8225),
            cdse_config=dict(min=243, max=255, mean=248.8026, median=248.0, std=3.8223),
        ),
        tile_num=dict(sh_config=17, cdse_config=17),
    ),
]


@pytest.mark.parametrize("test_case", TEST_CASES)
@pytest.mark.parametrize("config", ["sh_config", "cdse_config"])
def test_ogc(test_case: OgcTestCase, output_folder: str, config: SHConfig, request) -> None:
    # skip test cases unsupported on CDSE
    if config == "cdse_config" and test_case.name in CDSE_UNSUPPORTED_CASES:
        pytest.skip("unsupported cases on cdse")

    # Run data collection
    request = test_case.initialize_request(output_folder, config=request.getfixturevalue(config))
    data = test_case.collect_data(request)

    assert isinstance(data, list)
    result_len = test_case.result_len[config] if test_case.data_filter is None else len(test_case.data_filter)
    assert len(data) == result_len

    tile_iter = request.get_tiles()
    tile_n = len(list(tile_iter)) if tile_iter else None
    assert tile_n == test_case.tile_num[config]

    if test_case.date_check is not None:
        dates = OgcImageService().get_dates(request)
        assert len(dates) == test_case.result_len[config]
        assert test_case.date_check == dates[0]

    if test_case.data_filter is not None:
        if (test_case.data_filter[0] - test_case.data_filter[-1]) % test_case.result_len[config] == 0:
            assert_array_equal(data[0], data[-1], err_msg="First and last output should be equal")
        else:
            assert not np.array_equal(data[0], data[-1]), "First and last output should be different"

    img_stats = test_case.img_stats[config]
    assert_statistics_match(
        data[0],
        exp_min=img_stats["min"],
        exp_max=img_stats["max"],
        exp_mean=img_stats["mean"],
        exp_median=img_stats["median"],
        exp_std=img_stats["std"],
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

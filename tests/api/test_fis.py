import ast
import os
from dataclasses import dataclass
from typing import Any

import pytest
from shapely.geometry import Polygon

from sentinelhub import CRS, BBox, CustomUrlParam, DataCollection, FisRequest, Geometry, HistogramType, ResamplingType
from sentinelhub.exceptions import SHDeprecationWarning

EXPECTED_RESULTS_PATH = os.path.join(os.path.dirname(__file__), "..", "TestInputs", "test_fis_results.txt")
with open(EXPECTED_RESULTS_PATH, "r") as file:
    RESULTS = [ast.literal_eval(line.strip()) for line in file]

BBOX = BBox([14.00, 45.00, 14.03, 45.03], crs=CRS.WGS84)
GEOMETRY1 = Geometry(
    Polygon(
        [
            (465888.877326859, 5079639.436138632),
            (465885.3413983975, 5079641.524618266),
            (465882.9542217017, 5079647.166043535),
            (465888.8780175466, 5079668.703676634),
            (465888.877326859, 5079639.436138632),
        ]
    ),
    CRS(32633),
)
GEOMETRY2 = Geometry("POLYGON((-5.13 48, -5.23 48.09, -5.13 48.17, -5.03 48.08, -5.13 48))", CRS.WGS84)


@dataclass
class FisTestCase:
    kwargs: dict
    name: str
    raw_result: Any
    result_length: int
    save_data: bool = False

    def collect_data(self, output_folder):
        request = FisRequest(**self.kwargs, data_folder=output_folder)
        if self.save_data:
            request.save_data(redownload=True)
            return request.get_data(save_data=True)
        return request.get_data(redownload=True)


TEST_CASES = [
    FisTestCase(
        dict(
            data_collection=DataCollection.SENTINEL2_L1C,
            layer="TRUE-COLOR-S2-L1C",
            geometry_list=[GEOMETRY1],
            time=("2017-1-1", "2017-2-1"),
            resolution="50m",
            histogram_type=HistogramType.STREAMING,
            bins=5,
        ),
        name="geometry",
        raw_result=RESULTS[0],
        result_length=1,
    ),
    FisTestCase(
        dict(
            custom_url_params={
                CustomUrlParam.DOWNSAMPLING: ResamplingType.BICUBIC,
                CustomUrlParam.UPSAMPLING: "bicubic",
            },
            data_collection=DataCollection.SENTINEL2_L1C,
            layer="BANDS-S2-L1C",
            geometry_list=[BBOX],
            time="2017-1-1",
            resolution="50m",
            maxcc=0.2,
        ),
        name="bbox",
        raw_result=RESULTS[1],
        result_length=1,
    ),
    FisTestCase(
        dict(
            data_collection=DataCollection.LANDSAT_OT_L1,
            layer="BANDS-L8",
            geometry_list=[BBOX, GEOMETRY1],
            time=("2017-1-1", "2017-1-10"),
            resolution="100m",
            bins=32,
        ),
        name="list",
        raw_result=RESULTS[2],
        result_length=2,
        save_data=True,
    ),
    FisTestCase(
        dict(
            data_collection=DataCollection.SENTINEL2_L1C,
            layer="TRUE-COLOR-S2-L1C",
            geometry_list=[GEOMETRY2],
            time=("2017-10-1", "2017-10-2"),
            resolution="60m",
            bins=11,
            histogram_type=HistogramType.EQUALFREQUENCY,
        ),
        name="Polygon in WGS84",
        raw_result=RESULTS[3],
        result_length=1,
    ),
]


@pytest.mark.sh_integration
@pytest.mark.parametrize("test_case", TEST_CASES)
def test_fis(output_folder, test_case):
    with pytest.warns(SHDeprecationWarning):
        data = test_case.collect_data(output_folder)
    assert len(data) == test_case.result_length
    assert data == test_case.raw_result

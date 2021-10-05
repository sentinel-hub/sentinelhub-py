import numpy as np
import pytest
from pytest import approx

from sentinelhub import AwsTileRequest, AwsProductRequest, DataCollection

pytestmark = pytest.mark.aws_integration


def test_aws_tile(output_folder):
    request = AwsTileRequest(
        data_folder=output_folder, bands='B01, B05',
        metafiles='metadata,tileInfo,  productInfo, qi/MSK_TECQUA_B04,  auxiliary/ECMWFT ',
        tile='10UEV', time='2016-01-09', aws_index=0, data_collection=DataCollection.SENTINEL2_L1C
    )
    data = request.get_data(redownload=True, data_filter=[0] + list(range(2, 7)))
    assert isinstance(data, list)
    assert len(data) == 6
    assert np.mean(data[0]) == approx(1357.99, abs=1e-1)


def test_aws_product(output_folder):
    request = AwsProductRequest(
        data_folder=output_folder, bands='B10', metafiles='metadata,tileInfo,productInfo, datastrip/*/metadata',
        product_id='S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_20160103T171947'
    )
    data = request.get_data(save_data=True, redownload=True, max_threads=100)

    assert isinstance(data, list)
    assert len(data) == 51


def test_partial_aws_product(output_folder):
    request = AwsProductRequest(
        data_folder=output_folder, bands='B12', metafiles='manifest,preview/B02', tile_list=['1WCV'],
        product_id='S2A_MSIL1C_20171010T003621_N0205_R002_T01WCV_20171010T003615'
    )
    data = request.get_data(save_data=True, redownload=True, data_filter=[1])

    download_list = request.get_url_list()
    assert isinstance(download_list, list)
    assert len(download_list) == 3

    assert isinstance(data, list)
    assert len(data) == 1


def test_l2a_product(output_folder):
    request = AwsProductRequest(
        data_folder=output_folder, metafiles='metadata,tileInfo,productInfo, datastrip/*/metadata',
        product_id='S2A_MSIL2A_20180402T151801_N0207_R068_T33XWJ_20180402T202222'
    )
    data = request.get_data(save_data=True, redownload=True, data_filter=[20])

    download_list = request.get_url_list()
    assert isinstance(download_list, list)
    assert len(download_list) == 41

    assert isinstance(data, list)
    assert len(data) == 1
    assert np.mean(data[0]) == approx(16.91783, abs=1e-4)

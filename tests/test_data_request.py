import os

import pytest

from sentinelhub import WcsRequest, AwsProductRequest, DataCollection, BBox, CRS, get_file_list, get_folder_list


@pytest.fixture(name='wcs_request')
def wcs_request_fixture(output_folder):
    bbox = BBox((111.7, 8.655, 111.6, 8.688), crs=CRS.WGS84)
    return WcsRequest(
        data_folder=output_folder,
        bbox=bbox,
        data_collection=DataCollection.SENTINEL2_L1C,
        layer='TRUE-COLOR-S2-L1C'
    )


@pytest.mark.sh_integration
def test_init(output_folder, wcs_request):
    wcs_request.create_request(reset_wfs_iterator=True)  # This method is used by s2cloudless, don't rename it

    assert wcs_request.data_folder == output_folder

    filename_list = wcs_request.get_filename_list()
    assert isinstance(filename_list, list)
    assert all(isinstance(filename, str) for filename in filename_list)

    url_list = wcs_request.get_url_list()
    assert isinstance(url_list, list)
    assert all(isinstance(url, str) for url in url_list)

    assert wcs_request.is_valid_request()


@pytest.mark.sh_integration
def test_encoded_latest_result(wcs_request):
    result_list = wcs_request.get_data(decode_data=False, save_data=True)

    assert isinstance(result_list, list)
    assert len(result_list) == 1
    assert all(isinstance(result, bytes) for result in result_list)

    cached_result_list = wcs_request.get_data(decode_data=False)
    assert result_list == cached_result_list


@pytest.mark.aws_integration
def test_saving_responses(output_folder):
    product_id = 'S2A_MSIL1C_20180113T001101_N0206_R073_T55KGP_20180113T013328.SAFE'
    metafiles = 'inspire '

    aws_request = AwsProductRequest(bands='', metafiles=metafiles, safe_format=True, product_id=product_id)
    try:
        data = aws_request.get_data()
        assert isinstance(data, list)
        assert len(data) == 1
    except ValueError:
        assert False, 'get_data method with save_data=False should work without specifying data_folder'

    with pytest.raises(ValueError):
        aws_request.get_data(save_data=True, redownload=True)
    with pytest.raises(ValueError):
        aws_request.save_data()

    aws_request = AwsProductRequest(
        data_folder=output_folder, bands='', metafiles=metafiles, safe_format=True, product_id=product_id
    )
    try:
        data = aws_request.get_data(save_data=True)
        assert isinstance(data, list)
        assert len(data) == 1
        product_folder = os.path.join(output_folder, product_id)
        assert len(get_folder_list(product_folder)) == 5
        assert len(get_file_list(product_folder)) == 1
    except ValueError:
        assert False, 'Expected to obtain and save data'

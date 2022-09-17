import pytest

from sentinelhub import CRS, BBox, DataCollection, WcsRequest
from sentinelhub.download.models import DownloadResponse


@pytest.fixture(name="wcs_request")
def wcs_request_fixture(output_folder: str) -> WcsRequest:
    bbox = BBox((111.7, 8.655, 111.6, 8.688), crs=CRS.WGS84)
    return WcsRequest(
        data_folder=output_folder, bbox=bbox, data_collection=DataCollection.SENTINEL2_L1C, layer="TRUE-COLOR-S2-L1C"
    )


@pytest.mark.sh_integration
def test_init(output_folder: str, wcs_request: WcsRequest) -> None:
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
def test_encoded_latest_result(wcs_request: WcsRequest) -> None:
    result_list = wcs_request.get_data(decode_data=False, save_data=True)

    assert isinstance(result_list, list)
    assert len(result_list) == 1
    assert all(isinstance(result, DownloadResponse) for result in result_list)

    cached_result_list = wcs_request.get_data(decode_data=False)
    assert result_list == cached_result_list

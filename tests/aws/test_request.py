import os

import pytest

from sentinelhub.aws import AwsProductRequest


@pytest.mark.aws_integration
def test_saving_responses(output_folder: str) -> None:
    product_id = "S2A_MSIL1C_20180113T001101_N0206_R073_T55KGP_20180113T013328.SAFE"
    metafiles = "inspire "

    aws_request = AwsProductRequest(bands="", metafiles=metafiles, safe_format=True, product_id=product_id)
    try:
        data = aws_request.get_data()
        assert isinstance(data, list)
        assert len(data) == 1
    except ValueError:
        raise AssertionError("get_data method with save_data=False should work without specifying data_folder")

    with pytest.raises(ValueError):
        aws_request.get_data(save_data=True, redownload=True)
    with pytest.raises(ValueError):
        aws_request.save_data()

    aws_request = AwsProductRequest(
        data_folder=output_folder, bands="", metafiles=metafiles, safe_format=True, product_id=product_id
    )
    try:
        data = aws_request.get_data(save_data=True)
        assert isinstance(data, list)
        assert len(data) == 1
        product_folder = os.path.join(output_folder, product_id)
        assert len([f for f in os.scandir(product_folder) if f.is_dir()]) == 5
        assert len([f for f in os.scandir(product_folder) if f.is_file()]) == 1
    except ValueError:
        raise AssertionError("Expected to obtain and save data")

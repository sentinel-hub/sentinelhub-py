import os

import pytest

from sentinelhub import AwsConstants, AwsProductRequest, AwsTileRequest, DataCollection, read_data, write_data


@pytest.fixture(name="safe_folder", scope="session")
def safe_folder_fixture(input_folder):
    """Provides a folder each file contains a definition of a .SAFE structure on how it will be reconstructred from
    files from AWS S3 buckets."""
    return os.path.join(input_folder, "aws_safe")


TEST_CASES = [
    (
        "L1C_02.01",
        "S2A_OPER_PRD_MSIL1C_PDMC_20151218T020842_R115_V20151217T224602_20151217T224602",
        dict(
            bands=AwsConstants.S2_L1C_BANDS,
            metafiles=AwsConstants.S2_L1C_METAFILES,
            tile_list=["T59HNA"],
        ),
    ),
    (
        "L1C_02.01_tile",
        None,
        dict(
            tile="29KQB",
            time="2016-04-12",
            aws_index=None,
            data_collection=DataCollection.SENTINEL2_L1C,
        ),
    ),
    (
        "L1C_02.02",
        "S2A_OPER_PRD_MSIL1C_PDMC_20160606T232310_R121_V20160526T084351_20160526T084351.SAFE",
        dict(tile_list=["34HCF"]),
    ),
    ("L1C_02.04_old", "S2A_OPER_PRD_MSIL1C_PDMC_20160910T174323_R071_V20160701T204642_20160701T204643", {}),
    ("L1C_02.04", "S2A_MSIL1C_20170413T104021_N0204_R008_T31SCA_20170413T104021", {}),
    ("L1C_02.05", "S2A_MSIL1C_20171012T112111_N0205_R037_T29SQC_20171012T112713", {}),
    ("L1C_02.06", "S2A_MSIL1C_20180331T212521_N0206_R043_T07WFR_20180401T005612", {}),
    ("L1C_02.06_2", "S2A_MSIL1C_20181004T175151_N0206_R141_T18XVM_20190219T160358", {}),
    ("L1C_02.07", "S2A_MSIL1C_20190129T143751_N0207_R096_T20LLK_20190225T132350", {}),
    ("L1C_02.08", "S2B_MSIL1C_20200101T235239_N0208_R130_T57KUR_20200102T010610", {}),
    ("L1C_02.09", "S2B_MSIL1C_20200318T231549_N0209_R087_T60UWB_20200319T005909", {}),
    ("L1C_03.00", "S2A_MSIL1C_20210501T093031_N0300_R136_T35TLM_20210502T130645", {}),
    ("L1C_03.01", "S2B_MSIL1C_20210918T235129_N0301_R073_T59UPT_20210919T010528", {}),
    ("L1C_04.00", "S2A_MSIL1C_20220317T074651_N0400_R135_T36MZB_20220317T091114", {}),
    ("L2A_00.01", "S2A_MSIL2A_20170414T101021_N0001_R022_T33UUQ_20190508T121145", {}),  # Reprocessed by Sentinel Hub
    ("L2A_00.01_2", "S2B_MSIL2A_20181119T172619_N0001_R012_T14SPG_20200916T182702", {}),  # Reprocessed by Sentinel Hub
    ("L2A_02.01", "S2A_USER_PRD_MSIL2A_PDMC_20160310T041843_R138_V20160308T131142_20160308T131142", {}),
    ("L2A_02.05", "S2A_MSIL2A_20170827T105651_N0205_R094_T31WFN_20170827T105652", {}),  # L2A_02.04 is the same
    ("L2A_02.06", "S2B_MSIL2A_20180216T102059_N0206_R065_T35VLL_20180216T122659", {}),
    ("L2A_02.07", "S2A_MSIL2A_20180402T151801_N0207_R068_T33XWJ_20180402T202222", {}),
    ("L2A_02.08", "S2A_MSIL2A_20181005T104021_N0208_R008_T34WEU_20181007T220806", {}),
    ("L2A_02.09", "S2B_MSIL2A_20181029T093109_N0209_R136_T35UMQ_20181029T122414", {}),
    ("L2A_02.10", "S2B_MSIL2A_20181115T110319_N0210_R094_T32VLJ_20181115T142501", {}),
    ("L2A_02.11", "S2B_MSIL2A_20190310T235739_N0211_R030_T57MVM_20190311T013927", {}),
    ("L2A_02.12", "S2B_MSIL2A_20190601T133729_N0212_R067_T30XWK_20190603T172501", {}),
    ("L2A_02.13", "S2B_MSIL2A_20200101T235239_N0213_R130_T56GMT_20200102T014048", {}),
    ("L2A_02.14", "S2A_MSIL2A_20200318T190111_N0214_R013_T19XEH_20200318T231533", {}),
    ("L2A_03.00", "S2B_MSIL2A_20210501T083559_N0300_R064_T35QQE_20210502T160359", {}),
    ("L2A_03.01", "S2B_MSIL2A_20210918T234619_N0301_R073_T59VPG_20210919T012911", {}),
    ("L2A_04.00", "S2B_MSIL2A_20220126T084129_N0400_R064_T36RUT_20220201T122115", {}),
]


@pytest.mark.aws_integration
@pytest.mark.parametrize("test_name, product_id, params", TEST_CASES)
def test_safe_struct(test_name, product_id, params, safe_folder):
    params = dict(
        safe_format=True,
        **params,
    )
    request = AwsTileRequest(**params) if product_id is None else AwsProductRequest(product_id, **params)
    safe_structure = [(req.url, req.filename) for req in request.get_download_list()]

    aws_safe_path = os.path.join(safe_folder, f"{test_name}.csv")

    # Uncomment the following to update a test or add a new one:
    # write_data(aws_safe_path, safe_structure)

    expected_safe_structure = [tuple(item) for item in read_data(aws_safe_path)]

    assert safe_structure == expected_safe_structure

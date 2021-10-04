import os
from dataclasses import dataclass
from typing import Union, Type

import pytest

from sentinelhub import AwsTileRequest, AwsProductRequest, read_data, write_data, DataCollection, AwsConstants
from sentinelhub.testing_utils import get_input_folder

INPUT_FOLDER = get_input_folder(__file__)


@dataclass
class SafeTestCase:
    name: str
    constructor: Union[Type[AwsTileRequest], Type[AwsProductRequest]]
    args: list
    kwargs: dict

    def get_filename(self):
        return os.path.join(INPUT_FOLDER, f'{self.name}.csv')

    def get_request_data(self, return_request=False):
        request = self.constructor(*self.args, **self.kwargs)
        request_data = [(req.url, req.filename[:]) for req in request.get_download_list()]
        if return_request:
            return request, request_data
        return request_data

    def load_truth(self):
        return [tuple(item) for item in read_data(self.get_filename())]

    def save_test_case(self):
        """ Use this method only to create new tests
        """
        request, request_data = self.get_request_data(return_request=True)
        write_data(self.get_filename(), request_data)
        request.save_data()


TEST_CASES = [
    SafeTestCase(
        'L1C_02.01', AwsProductRequest,
        ['S2A_OPER_PRD_MSIL1C_PDMC_20151218T020842_R115_V20151217T224602_20151217T224602'],
        dict(
            bands=AwsConstants.S2_L1C_BANDS, metafiles=AwsConstants.S2_L1C_METAFILES, tile_list=['T59HNA'],
            safe_format=True, data_folder=INPUT_FOLDER
        )
    ),
    SafeTestCase(
        'L1C_02.01_tile', AwsTileRequest, [],
        dict(
            tile='29KQB', time='2016-04-12', aws_index=None, data_collection=DataCollection.SENTINEL2_L1C,
            safe_format=True, data_folder=INPUT_FOLDER
        )
    ),
    SafeTestCase(
        'L1C_02.02', AwsProductRequest,
        ['S2A_OPER_PRD_MSIL1C_PDMC_20160606T232310_R121_V20160526T084351_20160526T084351.SAFE'],
        dict(tile_list=['34HCF'], safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L1C_02.04_old', AwsProductRequest,
        ['S2A_OPER_PRD_MSIL1C_PDMC_20160910T174323_R071_V20160701T204642_20160701T204643'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L1C_02.04', AwsProductRequest, ['S2A_MSIL1C_20170413T104021_N0204_R008_T31SCA_20170413T104021'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L1C_02.05', AwsProductRequest, ['S2A_MSIL1C_20171012T112111_N0205_R037_T29SQC_20171012T112713'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L1C_02.06', AwsProductRequest, ['S2A_MSIL1C_20180331T212521_N0206_R043_T07WFR_20180401T005612'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L1C_02.06_2', AwsProductRequest, ['S2A_MSIL1C_20181004T175151_N0206_R141_T18XVM_20190219T160358'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L1C_02.07', AwsProductRequest, ['S2A_MSIL1C_20181119T031011_N0207_R075_T50TLK_20181119T061056'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L1C_02.07_2', AwsProductRequest, ['S2A_MSIL1C_20190129T143751_N0207_R096_T20LLK_20190225T132350'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L2A_02.01', AwsProductRequest,
        ['S2A_USER_PRD_MSIL2A_PDMC_20160310T041843_R138_V20160308T131142_20160308T131142'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(  # L2A_02.04 is the same
        'L2A_02.05', AwsProductRequest, ['S2A_MSIL2A_20170827T105651_N0205_R094_T31WFN_20170827T105652'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L2A_02.06', AwsProductRequest, ['S2B_MSIL2A_20180216T102059_N0206_R065_T35VLL_20180216T122659'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L2A_02.07', AwsProductRequest, ['S2A_MSIL2A_20180402T151801_N0207_R068_T33XWJ_20180402T202222'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L2A_02.08', AwsProductRequest, ['S2A_MSIL2A_20181005T104021_N0208_R008_T34WEU_20181007T220806'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L2A_02.09', AwsProductRequest, ['S2B_MSIL2A_20181029T093109_N0209_R136_T35UMQ_20181029T122414'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L2A_02.10', AwsProductRequest, ['S2B_MSIL2A_20181115T110319_N0210_R094_T32VLJ_20181115T142501'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L2A_02.11', AwsProductRequest, ['S2B_MSIL2A_20190310T235739_N0211_R030_T57MVM_20190311T013927'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
    SafeTestCase(
        'L2A_00.01', AwsProductRequest, ['S2A_MSIL2A_20170414T101021_N0001_R022_T33UUQ_20190508T121145'],
        dict(safe_format=True, data_folder=INPUT_FOLDER)
    ),
]


# Uncomment the following only when creating new test cases
# for test_case in TEST_CASES:
#     test_case.save_test_case()

@pytest.mark.aws_integration
@pytest.mark.parametrize('test_case', TEST_CASES)
def test_safe_struct(test_case):
    true_safe = test_case.load_truth()
    req_safe = test_case.get_request_data()
    assert true_safe == req_safe

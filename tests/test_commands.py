import subprocess
import os

import pytest

from sentinelhub.testing_utils import get_output_folder

OUTPUT_FOLDER = get_output_folder(__file__)
COMPACT_PRODUCT_ID = 'S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551'
OLD_PRODUCT_ID = 'S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_20160103T171947'
L2A_PRODUCT_ID = 'S2A_MSIL2A_20180402T151801_N0207_R068_T33XWJ_20180402T202222'
L1C_TILE = 'T38TML 2015-12-19'
L2A_TILE = 'T33XWJ 2018-04-02'
URL = 'http://roda.sentinel-hub.com/sentinel-s2-l1c/tiles/54/H/VH/2017/4/14/0/metadata.xml'


@pytest.mark.parametrize('command', [
    pytest.param(f'sentinelhub.aws --product {COMPACT_PRODUCT_ID} -ri -b B8A', marks=pytest.mark.aws_integration),
    pytest.param(f'sentinelhub.aws --product {OLD_PRODUCT_ID} -i', marks=pytest.mark.aws_integration),
    pytest.param(f'sentinelhub.aws --product {L2A_PRODUCT_ID} -i', marks=pytest.mark.aws_integration),
    pytest.param(f'sentinelhub.aws --tile {L1C_TILE} -rei --bands B01,B10', marks=pytest.mark.aws_integration),
    pytest.param(f'sentinelhub.aws --tile {L2A_TILE} --l2a -f {OUTPUT_FOLDER}', marks=pytest.mark.aws_integration),
    pytest.param(f'sentinelhub.download {URL} {os.path.join(OUTPUT_FOLDER, "example.xml")} -r',
                 marks=pytest.mark.sh_integration),
    'sentinelhub.config --show',
    'sentinelhub --help',
    'sentinelhub.aws --help',
    'sentinelhub.config --help',
    'sentinelhub.download --help',
])
def test_return_type(output_folder, command):
    assert subprocess.call(command, shell=True) == 0

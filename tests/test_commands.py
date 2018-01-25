import unittest
import subprocess
import os
from tests_all import TestSentinelHub


class TestCommands(TestSentinelHub):
    @classmethod
    def setUpClass(cls):
        cls.status = 0

        if not os.path.exists(cls.OUTPUT_FOLDER):
            os.mkdir(cls.OUTPUT_FOLDER)

        compact_product_id = 'S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551'
        cls.status += subprocess.call('sentinelhub.aws --product {} -rf ./{}'.format(compact_product_id,
                                                                                     cls.OUTPUT_FOLDER), shell=True)
        old_product_id = 'S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_20160103T171947'
        cls.status += subprocess.call('sentinelhub.aws --product {} -ib B8A,B06'.format(old_product_id), shell=True)
        cls.status += subprocess.call('sentinelhub.aws --tile T38TML 2015-12-19 -ref {} '
                                      '--bands B08,B11'.format(cls.OUTPUT_FOLDER), shell=True)
        url = 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/54/H/VH/2017/4/14/0/metadata.xml'
        cls.status += subprocess.call('sentinelhub.download {} {}/example.xml -r'.format(url, cls.OUTPUT_FOLDER),
                                      shell=True)

    def test_return_type(self):
        self.assertTrue(self.status == 0, "Commands failed")


if __name__ == '__main__':
    unittest.main()

import logging
import unittest
import shutil
import subprocess

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)-15s %(module)s:%(lineno)d [%(levelname)s] %(funcName)s  %(message)s')


class TestCommands(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.status = 0
        folder = 'TestOutputs'
        compact_product_id = 'S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551'
        cls.status += subprocess.call('sentinelhub.aws --product {} -rf ./{}'.format(compact_product_id, folder),
                                      shell=True)
        old_product_id = 'S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_20160103T171947'
        cls.status += subprocess.call('sentinelhub.aws --product {} -ib B8A,B06'.format(old_product_id), shell=True)
        cls.status += subprocess.call('sentinelhub.aws --tile T38TML 2015-12-19 -ref {} --bands B08,B11'.format(folder),
                                      shell=True)
        url = 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/54/H/VH/2017/4/14/0/metadata.xml'
        cls.status += subprocess.call('sentinelhub.download {} {}/example.xml -r'.format(url, folder), shell=True)

    def test_return_type(self):
        self.assertTrue(self.status == 0, "Commands failed")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('./TestOutputs/', ignore_errors=True)


if __name__ == '__main__':
    unittest.main()

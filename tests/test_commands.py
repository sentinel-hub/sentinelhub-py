import unittest
import subprocess
import os

from sentinelhub import TestSentinelHub


class TestCommands(TestSentinelHub):
    @classmethod
    def setUpClass(cls):

        if not os.path.exists(cls.OUTPUT_FOLDER):
            os.mkdir(cls.OUTPUT_FOLDER)

        compact_product_id = 'S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551'
        old_product_id = 'S2A_OPER_PRD_MSIL1C_PDMC_20160121T043931_R069_V20160103T171947_20160103T171947'
        l2a_product_id = 'S2A_MSIL2A_20180402T151801_N0207_R068_T33XWJ_20180402T202222'
        l1c_tile = 'T38TML 2015-12-19'
        l2a_tile = 'T33XWJ 2018-04-02'
        url = 'http://roda.sentinel-hub.com/sentinel-s2-l1c/tiles/54/H/VH/2017/4/14/0/metadata.xml'

        cls.commands = [
            'sentinelhub.aws --product {} -ri -b B8A'.format(compact_product_id),
            'sentinelhub.aws --product {} -i'.format(old_product_id),
            'sentinelhub.aws --product {} -i'.format(l2a_product_id),
            'sentinelhub.aws --tile {} -rei --bands B01,B10'.format(l1c_tile),
            'sentinelhub.aws --tile {} --l2a -f {}'.format(l2a_tile, cls.OUTPUT_FOLDER),
            'sentinelhub.download {} {} -r'.format(url, os.path.join(cls.OUTPUT_FOLDER, 'example.xml')),
            'sentinelhub.config --show',
            'sentinelhub --help',
            'sentinelhub.aws --help',
            'sentinelhub.config --help',
            'sentinelhub.download --help'
        ]

    def test_return_type(self):
        for command in self.commands:
            with self.subTest(msg='Test case {}'.format(command)):
                self.assertTrue(subprocess.call(command, shell=True) == 0, 'Failed command: {} '.format(command))


if __name__ == '__main__':
    unittest.main()

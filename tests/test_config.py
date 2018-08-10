import unittest
import json
import os.path
from tests_all import TestSentinelHub

from sentinelhub import SHConfig


class TestSHConfig(TestSentinelHub):
    def test_configuration(self):
        SHConfig().save()

        config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'sentinelhub', 'config.json')

        if not os.path.isfile(config_file):
            self.fail(msg='Config file does not exist: {}'.format(os.path.abspath(config_file)))

        with open(config_file, 'r') as fp:
            config = json.load(fp)

        for attr in config:
            if attr not in ['instance_id', 'aws_access_key_id', 'aws_secret_access_key']:
                self.assertEqual(SHConfig()[attr], config[attr],
                                 "Expected value {}, got {}".format(config[attr], SHConfig()[attr]))


if __name__ == '__main__':
    unittest.main()

import unittest
import json
import os

from sentinelhub import SHConfig, TestSentinelHub


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

    def test_reset(self):
        config = SHConfig()

        old_value = config.instance_id
        new_value = 'new'
        config.instance_id = new_value
        self.assertEqual(config.instance_id, new_value, 'New value was not set')
        self.assertEqual(config['instance_id'], new_value, 'New value was not set')
        self.assertEqual(config._instance.instance_id, old_value, 'Private value has changed')

        config.reset('ogc_base_url')
        config.reset(['aws_access_key_id', 'aws_secret_access_key'])
        self.assertEqual(config.instance_id, new_value, 'Instance ID should not reset yet')
        config.reset()
        self.assertEqual(config.instance_id, config._instance.CONFIG_PARAMS['instance_id'],
                         'Instance ID should reset')


if __name__ == '__main__':
    unittest.main()

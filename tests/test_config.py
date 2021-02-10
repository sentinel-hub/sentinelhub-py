import unittest
import json
import os

from sentinelhub import SHConfig, TestSentinelHub


class TestSHConfig(TestSentinelHub):
    def test_configuration(self):
        SHConfig().save()

        config_file = SHConfig().get_config_location()

        if not os.path.isfile(config_file):
            self.fail(msg='Config file does not exist: {}'.format(os.path.abspath(config_file)))

        with open(config_file, 'r') as fp:
            config = json.load(fp)

        for attr in config:
            if attr not in ['instance_id', 'aws_access_key_id', 'aws_secret_access_key',
                            'sh_client_id', 'sh_client_secret']:
                value = config[attr]
                if isinstance(value, str):
                    value = value.rstrip('/')
                self.assertEqual(SHConfig()[attr], value,
                                 "Expected value {}, got {}".format(config[attr], SHConfig()[attr]))

    def test_reset(self):
        config = SHConfig()

        old_value = config.instance_id
        new_value = 'new'
        config.instance_id = new_value
        self.assertEqual(config.instance_id, new_value, 'New value was not set')
        self.assertEqual(config['instance_id'], new_value, 'New value was not set')
        self.assertEqual(config._instance.instance_id, old_value, 'Private value has changed')

        config.reset('sh_base_url')
        config.reset(['aws_access_key_id', 'aws_secret_access_key'])
        self.assertEqual(config.instance_id, new_value, 'Instance ID should not reset yet')
        config.reset()
        self.assertEqual(config.instance_id, config._instance.CONFIG_PARAMS['instance_id'],
                         'Instance ID should reset')

    def test_save(self):
        config = SHConfig()
        old_value = config.download_timeout_seconds
        config.download_timeout_seconds = "abcd"
        self.assertRaises(ValueError, config.save)
        new_value = 150.5
        config.download_timeout_seconds = new_value
        config.save()
        config = SHConfig()
        self.assertEqual(config.download_timeout_seconds, new_value, "Saved value has not changed")
        config.download_timeout_seconds = old_value
        config.save()

if __name__ == '__main__':
    unittest.main()

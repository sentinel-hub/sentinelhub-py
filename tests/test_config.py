import unittest
import json
import os.path
from tests_all import TestSentinelHub

from sentinelhub.config import SHConfig


class TestSHConfig(TestSentinelHub):
    def test_configuration(self):
        SHConfig().save()

        config_file = '{}/../sentinelhub/config.json'.format(os.path.dirname(os.path.realpath(__file__)))

        if not os.path.isfile(config_file):
            self.fail(msg='Config file does not exist: {}'.format(os.path.abspath(config_file)))

        with open(config_file, 'r') as fp:
            config = json.load(fp)

            ogc_base_url = config['ogc_base_url']
            aws_base_url = config['aws_base_url']
            default_start_date = config['default_start_date']
            max_download_attempts = config['max_download_attempts']
            download_sleep_time = config['download_sleep_time']

            instance_id = config['instance_id']
            if not instance_id:
                instance_id = None

        self.assertEqual(SHConfig().ogc_base_url, ogc_base_url,
                         msg="Expected {}, got {}".format(ogc_base_url, SHConfig().ogc_base_url))
        self.assertEqual(SHConfig().aws_base_url, aws_base_url,
                         msg="Expected {}, got {}".format(aws_base_url, SHConfig().aws_base_url))
        self.assertEqual(SHConfig().instance_id, instance_id,
                         msg="Expected {}, got {}".format(instance_id, SHConfig().instance_id))
        self.assertEqual(SHConfig().default_start_date, default_start_date,
                         msg="Expected {}, got {}".format(default_start_date, SHConfig().default_start_date))
        self.assertEqual(SHConfig().max_download_attempts, max_download_attempts,
                         msg="Expected {}, got {}".format(max_download_attempts,
                                                          SHConfig().max_download_attempts))
        self.assertEqual(SHConfig().download_sleep_time, download_sleep_time,
                         msg="Expected {}, got {}".format(download_sleep_time,
                                                          SHConfig().download_sleep_time))


if __name__ == '__main__':
    unittest.main()

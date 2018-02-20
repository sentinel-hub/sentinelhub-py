import unittest
import json
import os.path
from tests_all import TestSentinelHub

from sentinelhub.config import SGConfig


class TestSGConfig(TestSentinelHub):
    def test_configuration(self):
        config_file = '{}/../sentinelhub/config.json'.format(os.path.dirname(os.path.realpath(__file__)))

        if not os.path.isfile(config_file):
            self.fail(msg='Config file does not exist: {}'.format(os.path.abspath(config_file)))

        with open(config_file, 'r') as fp:
            config = json.load(fp)

            ogc_base_url = config['ogc_base_url']
            aws_base_url = config['aws_base_url']
            aws_website_url = config['aws_website_url']
            default_start_date = config['default_start_date']
            max_download_attempts = config['max_download_attempts']
            download_sleep_time = config['download_sleep_time']

            instance_id = config['instance_id']
            if not instance_id:
                instance_id = None

        self.assertEqual(SGConfig().ogc_base_url, ogc_base_url,
                         msg="Expected {}, got {}".format(ogc_base_url, SGConfig().ogc_base_url))
        self.assertEqual(SGConfig().aws_base_url, aws_base_url,
                         msg="Expected {}, got {}".format(aws_base_url, SGConfig().aws_base_url))
        self.assertEqual(SGConfig().aws_website_url, aws_website_url,
                         msg="Expected {}, got {}".format(aws_website_url, SGConfig().aws_website_url))
        self.assertEqual(SGConfig().instance_id, instance_id,
                         msg="Expected {}, got {}".format(instance_id, SGConfig().instance_id))
        self.assertEqual(SGConfig().default_start_date, default_start_date,
                         msg="Expected {}, got {}".format(default_start_date, SGConfig().default_start_date))
        self.assertEqual(SGConfig().max_download_attempts, max_download_attempts,
                         msg="Expected {}, got {}".format(max_download_attempts,
                                                          SGConfig().max_download_attempts))
        self.assertEqual(SGConfig().download_sleep_time, download_sleep_time,
                         msg="Expected {}, got {}".format(download_sleep_time,
                                                          SGConfig().download_sleep_time))


if __name__ == '__main__':
    unittest.main()

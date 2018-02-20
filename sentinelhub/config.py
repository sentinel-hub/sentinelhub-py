"""
Module that collects configuration data from config.json
"""

import os.path
import json

# pylint: disable=E0203


class SGConfig:
    """ This is a singleton implementation of the sentinelhub configuration class.

    The class reads during its first initialisation the configurable settings from
    ``./config.json`` file:

        - instance_id: users' instance id. User can set it to his/hers instance id in ``config.json`` instead
          of specifying it explicitly every time he/she creates new ogc request.
        - ogc_base_url: base url for Sentinel Hub's services (should not be changed by the user).
        - aws_base_url: base url for Sentinel-2 data on AWS (should not be changed by the user).
        - aws_website_url: base url for AWS' public Sentinel-2 image browser.
        - opensearch_url: base url for Sentinelhub Opensearch service.
        - max_wfs_records_per_query: maximum number of records returned for each WFS query.
        - max_opensearch_records_per_query: maximum number of records returned for each Opensearch query.
        - default_start_date: In case time parameter for OGC data requests is not specified this will be used for
          start date of the interval.
        - download_timeout_seconds: maximum number of seconds before download attempt is canceled.

    Usage in the code:

        * ``SGConfig().ogc_base_url``
        * ``SGConfig().instance_id``

    """
    class _SGConfig:
        """
        Private class.
        """
        def __init__(self):
            self.config_params = ['ogc_base_url', 'instance_id', 'aws_base_url',
                                  'aws_website_url', 'opensearch_url', 'max_wfs_records_per_query',
                                  'max_opensearch_records_per_query', 'default_start_date',
                                  'max_download_attempts', 'download_sleep_time', 'download_timeout_seconds']
            self._load_configuration()

        def _check_configuration(self, config):
            """
            Checks if configuration file has contains all keys.

            :param config: configuration dictionary read from ``config.json``
            :type config: dict
            """

            for key in self.config_params:
                if key not in config:
                    raise ValueError('Configuration file does not contain %s key.' % key)
            if config['max_wfs_records_per_query'] > 100:
                raise ValueError("Value of config parameter 'max_wfs_records_per_query' must be at most 100")
            if config['max_opensearch_records_per_query'] > 500:
                raise ValueError("Value of config parameter 'max_opensearch_records_per_query' must be at most 500")

        def _load_configuration(self):
            """
            Method reads and loads the configuration file.
            """
            sentinelhub_dir = os.path.dirname(__file__)
            config_file = os.path.join(sentinelhub_dir, 'config.json')

            if not os.path.isfile(config_file):
                raise IOError('Configuration file does not exist: %s' % os.path.abspath(config_file))

            with open(config_file, 'r') as cfg_file:
                config = json.load(cfg_file)
                self._check_configuration(config)

                for prop in config:
                    if prop in self.config_params:
                        setattr(self, prop, config[prop])

                if not self.instance_id:
                    self.instance_id = None

    instance = None

    def __init__(self):
        if not SGConfig.instance:
            SGConfig.instance = SGConfig._SGConfig()

    def __getattr__(self, name):
        return getattr(self.instance, name)

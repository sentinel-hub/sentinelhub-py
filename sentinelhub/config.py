"""
Module that collects configuration data from config.json
"""

import os.path
import json
from collections import OrderedDict


class SHConfig:
    """ This is a singleton implementation of the sentinelhub configuration class.

    The class reads during its first initialization the configurable settings from
    ``./config.json`` file:

        - `instance_id`: Users' instance id. User can set it to his/hers instance id in ``config.json`` instead
          of specifying it explicitly every time he/she creates new ogc request.
        - `aws_access_key_id`: Access key for AWS Requester Pays buckets.
        - `aws_secret_access_key`: Secret access key for AWS Requester Pays buckets.
        - `ogc_base_url`: Base url for Sentinel Hub's services (should not be changed by the user).
        - `gpd_base_url`: Base url for Geopedia's services (should not be changed by the user).
        - `aws_metadata_base_url`: Base url for publicly available metadata files
        - `aws_s3_l1c_bucket`: Name of Sentinel-2 L1C bucket at AWS s3 service.
        - `aws_s3_l2a_bucket`: Name of Sentinel-2 L2A bucket at AWS s3 service.
        - `opensearch_url`: Base url for Sentinelhub Opensearch service.
        - `max_wfs_records_per_query`: Maximum number of records returned for each WFS query.
        - `max_opensearch_records_per_query`: Maximum number of records returned for each Opensearch query.
        - `default_start_date`: In case time parameter for OGC data requests is not specified this will be used for
          start date of the interval.
        - `max_download_attempts`: Maximum number of download attempts from a single URL until an error will be raised.
        - `download_sleep_time`: Number of seconds between the failed download attempt and the next attempt.
        - `download_timeout_seconds`: Maximum number of seconds before download attempt is canceled.

    Usage in the code:

        * ``SHConfig().ogc_base_url``
        * ``SHConfig().instance_id``

    """
    class _SHConfig:
        """
        Private class.
        """
        CONFIG_PARAMS = OrderedDict([
            ('instance_id', str),
            ('aws_access_key_id', str),
            ('aws_secret_access_key', str),
            ('ogc_base_url', str),
            ('gpd_base_url', str),
            ('aws_metadata_base_url', str),
            ('aws_s3_l1c_bucket', str),
            ('aws_s3_l2a_bucket', str),
            ('opensearch_url', str),
            ('max_wfs_records_per_query', int),
            ('max_opensearch_records_per_query', int),
            ('default_start_date', str),
            ('max_download_attempts', int),
            ('download_sleep_time', int),
            ('download_timeout_seconds', int)
        ])

        def __init__(self):
            self.instance_id = ''
            self.load_configuration()

        def _check_configuration(self, config):
            """
            Checks if configuration file has contains all keys.

            :param config: configuration dictionary read from ``config.json``
            :type config: dict
            """

            for param in self.CONFIG_PARAMS:
                if param not in config:
                    raise ValueError("Configuration file does not contain '%s' parameter." % param)
            for param, param_type in self.CONFIG_PARAMS.items():
                if not isinstance(config[param], param_type):
                    raise ValueError("Value of parameter '{}' must be of type {}".format(param, param_type.__name__))
            if config['max_wfs_records_per_query'] > 100:
                raise ValueError("Value of config parameter 'max_wfs_records_per_query' must be at most 100")
            if config['max_opensearch_records_per_query'] > 500:
                raise ValueError("Value of config parameter 'max_opensearch_records_per_query' must be at most 500")

        @staticmethod
        def get_config_file():
            """Checks if configuration file exists and returns its file path

            :return: location of configuration file
            :rtype: str
            """
            config_file = os.path.join(os.path.dirname(__file__), 'config.json')

            if not os.path.isfile(config_file):
                raise IOError('Configuration file does not exist: %s' % os.path.abspath(config_file))

            return config_file

        def load_configuration(self):
            """
            Method reads and loads the configuration file.
            """
            with open(self.get_config_file(), 'r') as cfg_file:
                config = json.load(cfg_file)
                self._check_configuration(config)

                for prop in config:
                    if prop in self.CONFIG_PARAMS:
                        setattr(self, prop, config[prop])

        def get_config(self):
            """Returns ordered dictionary with configuration parameters

            :return: Ordered dictionary
            :rtype: collections.OrderedDict
            """
            config = OrderedDict((prop, getattr(self, prop)) for prop in self.CONFIG_PARAMS)
            if config['instance_id'] is None:
                config['instance_id'] = ''
            return config

        def save_configuration(self):
            """
            Method saves changed parameter values to the configuration file.
            """
            config = self.get_config()

            self._check_configuration(config)

            with open(self.get_config_file(), 'w') as cfg_file:
                json.dump(config, cfg_file, indent=2)

    _instance = None

    def __init__(self):
        if not SHConfig._instance:
            SHConfig._instance = self._SHConfig()

    def __getattr__(self, name):
        return getattr(self._instance, name)

    def __getitem__(self, name):
        return getattr(self._instance, name)

    def __dir__(self):
        return sorted(list(dir(super())) + list(self._instance.CONFIG_PARAMS))

    def __str__(self):
        return json.dumps(self._instance.get_config(), indent=2)

    def save(self):
        """Method that saves configuration parameter changes from instance of SHConfig class to global config class and
        to ``config.json`` file.

        Example of use case
            ``my_config = SHConfig()`` \n
            ``my_config.instance_id = '<new instance id>'`` \n
            ``my_config.save()``
        """
        is_changed = False
        for prop in self._instance.CONFIG_PARAMS:
            if getattr(self, prop) != getattr(self._instance, prop):
                is_changed = True
                setattr(self._instance, prop, getattr(self, prop))
        if is_changed:
            self._instance.save_configuration()

    def get_params(self):
        """Returns a list of parameter names

        :return: List of parameter names
        :rtype: list(str)
        """
        return list(self._instance.CONFIG_PARAMS)

    def get_config_location(self):
        """ Returns location of configuration file on disk

        :return: File path of config.json file
        :rtype: str
        """
        return self._instance.get_config_file()

    def is_eocloud_ogc_url(self):
        """ Checks if base OGC URL is set to eocloud URL

        :return: True if 'eocloud' string is in base OGC URL else False
        :rtype: bool
        """
        return 'eocloud' in self.ogc_base_url

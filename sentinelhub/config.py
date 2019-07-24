"""
Module for managing configuration data from `config.json`
"""

import os
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
        - `ogc_base_url`: Base url for Sentinel Hub's services.
        - `geopedia_wms_url`: Base url for Geopedia WMS services.
        - `geopedia_rest_url`: Base url for Geopedia REST services.
        - `aws_metadata_url`: Base url for publicly available metadata files
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
        """ Internal class holding configuration parameters
        """
        CONFIG_PARAMS = OrderedDict([
            ('instance_id', ''),
            ('aws_access_key_id', ''),
            ('aws_secret_access_key', ''),
            ('ogc_base_url', 'https://services.sentinel-hub.com/ogc/'),
            ('geopedia_wms_url', 'http://service.geopedia.world/'),
            ('geopedia_rest_url', 'https://www.geopedia.world/rest/'),
            ('aws_metadata_url', 'https://roda.sentinel-hub.com/'),
            ('aws_s3_l1c_bucket', 'sentinel-s2-l1c'),
            ('aws_s3_l2a_bucket', 'sentinel-s2-l2a'),
            ('opensearch_url', 'http://opensearch.sentinel-hub.com/resto/api/collections/Sentinel2/'),
            ('max_wfs_records_per_query', 100),
            ('max_opensearch_records_per_query', 500),
            ('default_start_date', '1985-01-01'),
            ('max_download_attempts', 4),
            ('download_sleep_time', 5),
            ('download_timeout_seconds', 120)
        ])

        def __init__(self):
            self.instance_id = ''
            self.load_configuration()

        def _check_configuration(self, config):
            """ Checks if configuration file contains all keys.

            :param config: configuration dictionary read from ``config.json``
            :type config: dict
            """
            for param in self.CONFIG_PARAMS:
                if param not in config:
                    raise ValueError("Configuration file does not contain '%s' parameter." % param)
            for param, default_param in self.CONFIG_PARAMS.items():
                param_type = type(default_param)
                if not isinstance(config[param], param_type):
                    raise ValueError("Value of parameter '{}' must be of type {}".format(param, param_type.__name__))
            if config['max_wfs_records_per_query'] > 100:
                raise ValueError("Value of config parameter 'max_wfs_records_per_query' must be at most 100")
            if config['max_opensearch_records_per_query'] > 500:
                raise ValueError("Value of config parameter 'max_opensearch_records_per_query' must be at most 500")

        @staticmethod
        def get_config_file():
            """ Checks if configuration file exists and returns its file path

            :return: location of configuration file
            :rtype: str
            """
            config_file = os.path.join(os.path.dirname(__file__), 'config.json')

            if not os.path.isfile(config_file):
                raise IOError('Configuration file does not exist: %s' % os.path.abspath(config_file))

            return config_file

        def load_configuration(self):
            """ Method reads and loads the configuration file.
            """
            with open(self.get_config_file(), 'r') as cfg_file:
                config = json.load(cfg_file)
                self._check_configuration(config)

                for prop in config:
                    if prop in self.CONFIG_PARAMS:
                        setattr(self, prop, config[prop])

        def get_config(self):
            """ Returns ordered dictionary with configuration parameters

            :return: Ordered dictionary
            :rtype: collections.OrderedDict
            """
            config = OrderedDict((prop, getattr(self, prop)) for prop in self.CONFIG_PARAMS)
            if config['instance_id'] is None:
                config['instance_id'] = ''
            return config

        def save_configuration(self):
            """ Method saves changed parameter values to the configuration file.
            """
            config = self.get_config()

            self._check_configuration(config)

            with open(self.get_config_file(), 'w') as cfg_file:
                json.dump(config, cfg_file, indent=2)

    _instance = None

    def __init__(self):
        if not SHConfig._instance:
            SHConfig._instance = self._SHConfig()

        for prop in self._instance.CONFIG_PARAMS:
            setattr(self, prop, getattr(self._instance, prop))

    def __getattr__(self, name):
        """ This is called only if the class doesn't have the attribute itself
        """
        return getattr(self._instance, name)

    def __getitem__(self, name):
        return getattr(self, name)

    def __dir__(self):
        return sorted(list(dir(super())) + list(self._instance.CONFIG_PARAMS))

    def __str__(self):
        """ Content of SHConfig in json schema
        """
        return json.dumps(self.get_config_dict(), indent=2)

    def __repr__(self):
        """ Representation of SHConfig parameters
        """
        repr_list = ['{}('.format(self.__class__.__name__)]

        for key, value in self.get_config_dict().items():
            repr_list.append('%s=%r,' % (key, value))

        return '\n  '.join(repr_list).strip(',') + '\n)'

    def save(self):
        """ Method that saves configuration parameter changes from instance of SHConfig class to global config class and
        to `config.json` file.

        :Example:
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

    def reset(self, params=...):
        """ Resets configuration class to initial values. Use `SHConfig.save()` method in order to save this change.

        :param params: Parameters which will be reset. Parameters can be specified with a list of names, e.g.
            ``['instance_id', 'aws_access_key_id', 'aws_secret_access_key']``, or as a single name, e.g.
            ``'ogc_base_url'``. By default all parameters will be reset and default value is ``Ellipsis``.
        :type params: Ellipsis or list(str) or str
        """
        if params is ...:
            params = self.get_params()
        if isinstance(params, str):
            self._reset_param(params)
        elif isinstance(params, (list, tuple)):
            for param in params:
                self._reset_param(param)
        else:
            raise ValueError('Parameters must be specified in form of a list of strings or as a single string, instead '
                             'got {}'.format(params))

    def _reset_param(self, param):
        """ Resets a single parameter

        :param param: A configuration parameter
        :type param: str
        """
        if param not in self._instance.CONFIG_PARAMS:
            raise ValueError("Cannot reset unknown parameter '{}'".format(param))
        setattr(self, param, self._instance.CONFIG_PARAMS[param])

    def get_params(self):
        """ Returns a list of parameter names

        :return: List of parameter names
        :rtype: list(str)
        """
        return list(self._instance.CONFIG_PARAMS)

    def get_config_dict(self):
        """ Get a dictionary representation of `SHConfig` class

        :return: A dictionary with configuration parameters
        :rtype: OrderedDict
        """
        return OrderedDict((prop, getattr(self, prop)) for prop in self._instance.CONFIG_PARAMS)

    def get_config_location(self):
        """ Returns location of configuration file on disk

        :return: File path of `config.json` file
        :rtype: str
        """
        return self._instance.get_config_file()

    def is_eocloud_ogc_url(self):
        """ Checks if base OGC URL is set to eocloud URL

        :return: `True` if 'eocloud' string is in base OGC URL else `False`
        :rtype: bool
        """
        return 'eocloud' in self.ogc_base_url

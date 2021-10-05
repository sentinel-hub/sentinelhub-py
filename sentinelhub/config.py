"""
Module for managing configuration data from `config.json`
"""

import os
import json
import numbers


class SHConfig:
    """ A sentinelhub-py package configuration class.

    The class reads during its first initialization the configurable settings from
    ``./config.json`` file:

        - `instance_id`: An instance ID for Sentinel Hub service used for OGC requests.
        - `sh_client_id`: User's OAuth client ID for Sentinel Hub service
        - `sh_client_secret`: User's OAuth client secret for Sentinel Hub service
        - `sh_base_url`: There exist multiple deployed instances of Sentinel Hub service, this parameter defines the
          location of a specific service instance.
        - `sh_auth_base_url`: Base url for Sentinel Hub Authentication service. Authentication is typically sent to the
          main service deployment even if `sh_base_url` points to another deployment.
        - `geopedia_wms_url`: Base url for Geopedia WMS services.
        - `geopedia_rest_url`: Base url for Geopedia REST services.
        - `aws_access_key_id`: Access key for AWS Requester Pays buckets.
        - `aws_secret_access_key`: Secret access key for AWS Requester Pays buckets.
        - `aws_metadata_url`: Base url for publicly available metadata files
        - `aws_s3_l1c_bucket`: Name of Sentinel-2 L1C bucket at AWS s3 service.
        - `aws_s3_l2a_bucket`: Name of Sentinel-2 L2A bucket at AWS s3 service.
        - `opensearch_url`: Base url for Sentinelhub Opensearch service.
        - `max_wfs_records_per_query`: Maximum number of records returned for each WFS query.
        - `max_opensearch_records_per_query`: Maximum number of records returned for each Opensearch query.
        - `max_download_attempts`: Maximum number of download attempts from a single URL until an error will be raised.
        - `download_sleep_time`: Number of seconds to sleep between the first failed attempt and the next. Every next
          attempt this number exponentially increases with factor `3`.
        - `download_timeout_seconds`: Maximum number of seconds before download attempt is canceled.
        - `number_of_download_processes`: Number of download processes, used to calculate rate-limit sleep time.

    Usage in the code:

        * ``SHConfig().sh_base_url``
        * ``SHConfig().instance_id``

    """
    class _SHConfig:
        """ Internal class holding configuration parameters
        """
        CONFIG_PARAMS = {
            'instance_id': '',
            'sh_client_id': '',
            'sh_client_secret': '',
            'sh_base_url': 'https://services.sentinel-hub.com',
            'sh_auth_base_url': 'https://services.sentinel-hub.com',
            'geopedia_wms_url': 'https://service.geopedia.world',
            'geopedia_rest_url': 'https://www.geopedia.world/rest',
            'aws_access_key_id': '',
            'aws_secret_access_key': '',
            'aws_metadata_url': 'https://roda.sentinel-hub.com',
            'aws_s3_l1c_bucket': 'sentinel-s2-l1c',
            'aws_s3_l2a_bucket': 'sentinel-s2-l2a',
            'opensearch_url': 'http://opensearch.sentinel-hub.com/resto/api/collections/Sentinel2',
            'max_wfs_records_per_query': 100,
            'max_opensearch_records_per_query': 500,
            'max_download_attempts': 4,
            'download_sleep_time': 5.0,
            'download_timeout_seconds': 120.0,
            'number_of_download_processes': 1
        }
        CREDENTIALS = {
            'instance_id',
            'sh_client_id',
            'sh_client_secret',
            'aws_access_key_id',
            'aws_secret_access_key'
        }

        def __init__(self):
            self.instance_id = ''
            self.load_configuration()

        def _parse_configuration(self, config):
            """ Checks if configuration file contains all keys and parses values
            """
            for param, default_value in self.CONFIG_PARAMS.items():
                if param not in config:
                    config[param] = default_value

            for param, default_param in self.CONFIG_PARAMS.items():
                param_type = type(default_param)
                if (param_type is float) and isinstance(config[param], numbers.Number):
                    continue
                if not isinstance(config[param], param_type):
                    raise ValueError(f"Value of parameter '{param}' must be of type {param_type.__name__}")

            if config['max_wfs_records_per_query'] > 100:
                raise ValueError("Value of config parameter 'max_wfs_records_per_query' must be at most 100")
            if config['max_opensearch_records_per_query'] > 500:
                raise ValueError("Value of config parameter 'max_opensearch_records_per_query' must be at most 500")

            # The following enables that url parameters can be written with or without / at the end
            for param, value in config.items():
                if isinstance(value, str) and value.startswith('http'):
                    config[param] = value.rstrip('/')

            return config

        def get_config_file(self):
            """ Checks if configuration file exists and returns its file path.
            If file doesn't exist it creates a default configurations file.

            :return: location of configuration file
            :rtype: str
            """
            config_file = os.path.join(os.path.dirname(__file__), 'config.json')

            if not os.path.isfile(config_file):
                with open(config_file, 'w') as cfg_file:
                    json.dump(self.CONFIG_PARAMS, cfg_file, indent=2)

            return config_file

        def load_configuration(self):
            """ Method reads and loads the configuration file.
            """
            with open(self.get_config_file(), 'r') as cfg_file:
                config = json.load(cfg_file)

            config = self._parse_configuration(config)

            for prop in config:
                if prop in self.CONFIG_PARAMS:
                    setattr(self, prop, config[prop])

        def get_config(self):
            """ Returns a dictionary with configuration parameters

            :return: A dictionary
            :rtype: dict
            """
            config = {item: getattr(self, item) for item in self.CONFIG_PARAMS}
            for item, default_value in self.CONFIG_PARAMS.items():
                if config[item] is None:
                    config[item] = default_value
            return config

        def save_configuration(self):
            """ Method saves changed parameter values to the configuration file.
            """
            config = self.get_config()

            self._parse_configuration(config)

            with open(self.get_config_file(), 'w') as cfg_file:
                json.dump(config, cfg_file, indent=2)

    _instance = None

    def __init__(self, hide_credentials=False):
        """
        :param hide_credentials: If `True` then methods that provide the entire content of the config object will mask
            out all credentials. But credentials could still be accessed directly from config object attributes. The
            default is `False`.
        :type hide_credentials: bool
        """
        self._hide_credentials = hide_credentials

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
        """ Content of SHConfig in json schema. If `hide_credentials` is set to `True` then credentials will be
        masked.
        """
        return json.dumps(self.get_config_dict(), indent=2)

    def __repr__(self):
        """ Representation of SHConfig parameters. If `hide_credentials` is set to `True` then credentials will be
        masked.
        """
        repr_list = [f'{self.__class__.__name__}(']

        for key, value in self.get_config_dict().items():
            repr_list.append(f'{key}={repr(value)},')

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
            ``'sh_base_url'``. By default all parameters will be reset and default value is ``Ellipsis``.
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
                             f'got {params}')

    def _reset_param(self, param):
        """ Resets a single parameter

        :param param: A configuration parameter
        :type param: str
        """
        if param not in self._instance.CONFIG_PARAMS:
            raise ValueError(f"Cannot reset unknown parameter '{param}'")
        setattr(self, param, self._instance.CONFIG_PARAMS[param])

    def get_params(self):
        """ Returns a list of parameter names

        :return: List of parameter names
        :rtype: list(str)
        """
        return list(self._instance.CONFIG_PARAMS)

    def get_config_dict(self):
        """ Get a dictionary representation of `SHConfig` class. If `hide_credentials` is set to `True` then
        credentials will be masked.

        :return: A dictionary with configuration parameters
        :rtype: dict
        """
        config_params = {param: getattr(self, param) for param in self._instance.CONFIG_PARAMS}

        if self._hide_credentials:
            config_params = {param: self._mask_credentials(param, value) for param, value in config_params.items()}

        return config_params

    def get_config_location(self):
        """ Returns location of configuration file on disk

        :return: File path of `config.json` file
        :rtype: str
        """
        return self._instance.get_config_file()

    def has_eocloud_url(self):
        """ Checks if base Sentinel Hub URL is set to eocloud URL

        :return: `True` if 'eocloud' string is in base OGC URL else `False`
        :rtype: bool
        """
        return 'eocloud' in self.sh_base_url

    def get_sh_oauth_url(self):
        """ Provides URL for Sentinel Hub authentication endpoint

        :return: An URL endpoint
        :rtype: str
        """
        return f'{self.sh_auth_base_url}/oauth/token'

    def get_sh_process_api_url(self):
        """  Provides URL for Sentinel Hub Process API endpoint

        :return: An URL endpoint
        :rtype: str
        """
        return f'{self.sh_base_url}/api/v1/process'

    def get_sh_ogc_url(self):
        """ Provides URL for Sentinel Hub OGC endpoint

        :return: An URL endpoint
        :rtype: str
        """
        ogc_endpoint = 'v1' if self.has_eocloud_url() else 'ogc'
        return f'{self.sh_base_url}/{ogc_endpoint}'

    def get_sh_rate_limit_url(self):
        """ Provides URL for Sentinel Hub rate limiting endpoint

        :return: An URL endpoint
        :rtype: str
        """
        return f'{self.sh_auth_base_url}/aux/ratelimit'

    def raise_for_missing_instance_id(self):
        """ In case Sentinel Hub instance ID is missing it raises an informative error

        :raises: ValueError
        """
        if not self.instance_id:
            raise ValueError('Sentinel Hub instance ID is missing. '
                             'Either provide it with SHConfig object or save it into config.json configuration file. '
                             'Check http://sentinelhub-py.readthedocs.io/en/latest/configure.html for more info.')

    def _mask_credentials(self, param, value):
        """ In case a credentials parameter is given it will mask its value
        """
        if not (param in self._instance.CREDENTIALS and value):
            return value
        if not isinstance(value, str):
            raise ValueError(f"Parameter '{param}' should be a string but {value} found")

        hide_size = min(max(len(value) - 4, 10), len(value))
        return '*' * hide_size + value[hide_size:]

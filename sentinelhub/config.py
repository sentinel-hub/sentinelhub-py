"""
Module for managing configuration data from `config.json`
"""
from __future__ import annotations

import copy
import json
import numbers
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Union


class SHConfig:  # pylint: disable=too-many-instance-attributes
    """A sentinelhub-py package configuration class.

    The class reads the configurable settings from ``config.json`` file on initialization:

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
        - `aws_session_token`: A session token for your AWS account. It is only needed when you are using temporary
          credentials.
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

    For manual modification of `config.json` you can see the expected location of the file via
    `SHConfig.get_config_location()`.

    Usage in the code:

        * ``SHConfig().sh_base_url``
        * ``SHConfig().instance_id``

    """

    CREDENTIALS = {
        "instance_id",
        "sh_client_id",
        "sh_client_secret",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
    }
    CONFIG_PARAMS = [
        "instance_id",
        "sh_client_id",
        "sh_client_secret",
        "sh_base_url",
        "sh_auth_base_url",
        "geopedia_wms_url",
        "geopedia_rest_url",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "aws_metadata_url",
        "aws_s3_l1c_bucket",
        "aws_s3_l2a_bucket",
        "opensearch_url",
        "max_wfs_records_per_query",
        "max_opensearch_records_per_query",
        "max_download_attempts",
        "download_sleep_time",
        "download_timeout_seconds",
        "number_of_download_processes",
    ]

    def __init__(self, hide_credentials: bool = True, use_defaults: bool = False):
        """
        :param hide_credentials: If `True` then methods that provide the entire content of the config object will mask
            out all credentials. But credentials could still be accessed directly from config object attributes.
        :param use_defaults: Does not load the configuration file, returns config object with defaults only.
        """

        self.instance_id: str = ""
        self.sh_client_id: str = ""
        self.sh_client_secret: str = ""
        self.sh_base_url: str = "https://services.sentinel-hub.com"
        self.sh_auth_base_url: str = "https://services.sentinel-hub.com"
        self.geopedia_wms_url: str = "https://service.geopedia.world"
        self.geopedia_rest_url: str = "https://www.geopedia.world/rest"
        self.aws_access_key_id: str = ""
        self.aws_secret_access_key: str = ""
        self.aws_session_token: str = ""
        self.aws_metadata_url: str = "https://roda.sentinel-hub.com"
        self.aws_s3_l1c_bucket: str = "sentinel-s2-l1c"
        self.aws_s3_l2a_bucket: str = "sentinel-s2-l2a"
        self.opensearch_url: str = "http://opensearch.sentinel-hub.com/resto/api/collections/Sentinel2"
        self.max_wfs_records_per_query: int = 100
        self.max_opensearch_records_per_query: int = 500  # pylint: disable=invalid-name
        self.max_download_attempts: int = 4
        self.download_sleep_time: float = 5.0
        self.download_timeout_seconds: float = 120.0
        self.number_of_download_processes: int = 1

        self._hide_credentials = hide_credentials

        if not use_defaults:
            loaded_instance = SHConfig.load()  # user parameters validated in here already
            for param in SHConfig.CONFIG_PARAMS:
                setattr(self, param, getattr(loaded_instance, param))

    def _validate_values(self) -> None:
        """Ensures that the values are aligned with expectations."""
        default = SHConfig(use_defaults=True)

        for param in self.CONFIG_PARAMS:
            value = getattr(self, param)
            default_value = getattr(default, param)
            param_type = type(default_value)

            if isinstance(value, str) and value.startswith("http"):
                value = value.rstrip("/")
            if (param_type is float) and isinstance(value, numbers.Number):
                continue
            if not isinstance(value, param_type):
                raise ValueError(f"Value of parameter `{param}` must be of type {param_type.__name__}")
        if self.max_wfs_records_per_query > 100:
            raise ValueError("Value of config parameter `max_wfs_records_per_query` must be at most 100")
        if self.max_opensearch_records_per_query > 500:
            raise ValueError("Value of config parameter `max_opensearch_records_per_query` must be at most 500")

    def __getitem__(self, name: str) -> Union[str, float]:
        """Config parameters can also be accessed as items."""
        if name in self.CONFIG_PARAMS:
            return getattr(self, name)
        raise KeyError(f"`{name}` is not a supported config parameter")

    def __str__(self) -> str:
        """Content of SHConfig in json schema. If `hide_credentials` is set to `True` then credentials are masked."""
        return json.dumps(self.get_config_dict(), indent=2)

    def __repr__(self) -> str:
        """Representation of SHConfig parameters. If `hide_credentials` is set to `True` then credentials are masked."""
        repr_list = [f"{self.__class__.__name__}("]

        for key, value in self.get_config_dict().items():
            repr_list.append(f"{key}={repr(value)},")

        return "\n  ".join(repr_list).strip(",") + "\n)"

    def __eq__(self, other: object) -> bool:
        """Two instances of `SHConfig` are equal if all values of their parameters are equal."""
        if not isinstance(other, SHConfig):
            return False
        return all(getattr(self, param) == getattr(other, param) for param in self.CONFIG_PARAMS)

    @classmethod
    def load(cls, filename: Optional[str] = None) -> SHConfig:
        """Loads configuration parameters from a file. If a filename is not specified the configuration is loaded from
        the location specified by `SHConfig.get_config_location()`.

        :param filename: Optional path of the configuration file to be loaded.
        """
        config = cls(use_defaults=True)

        if filename is None:
            filename = cls.get_config_location()
            if not os.path.exists(filename):  # nothing to load from disk
                config.save()  # store default configuration to standard location
                return config

        with open(filename, "r") as cfg_file:
            config_dict = json.load(cfg_file)

        for param, value in config_dict.items():
            if param in cls.CONFIG_PARAMS:
                setattr(config, param, value)

        config._validate_values()
        return config

    def save(self, filename: Optional[str] = None) -> None:
        """Saves configuration parameters to the user settings in the `config.json` file.  If a filename is not
        specified, the configuration is saved to the location specified by `SHConfig.get_config_location()`.

        :param filename: Optional path of the configuration file to be saved.
        """
        self._validate_values()

        config_dict = {param: getattr(self, param) for param in self.CONFIG_PARAMS}
        file_path = Path(filename or self.get_config_location())
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "w") as cfg_file:
            json.dump(config_dict, cfg_file, indent=2)

    def copy(self) -> SHConfig:
        """Makes a copy of an instance of `SHConfig`"""
        return copy.copy(self)

    def reset(self, params: Union[str, Iterable[str], object] = ...) -> None:
        """Resets configuration class to default values. Does not save unless the `save` method is called afterwards.

        Parameters can be specified as a list of names, e.g. ``['instance_id', 'aws_access_key_id']``, or as a single
        name, e.g. ``'sh_base_url'``. By default, all parameters will be reset.

        :param params: Parameters to reset to default values.
        """
        default_config = SHConfig(use_defaults=True)

        if params is ...:
            params = self.get_params()
        elif isinstance(params, str):
            params = [params]

        if isinstance(params, Iterable):
            for param in params:
                self._reset_param(param, default_config)
        else:
            raise ValueError(f"Parameters must be a list of strings or as a single string, got {params}")

    def _reset_param(self, param: str, default_config: SHConfig) -> None:
        """Resets a single parameter."""
        if param not in self.get_params():
            raise ValueError(f"Cannot reset unknown parameter `{param}`")
        setattr(self, param, getattr(default_config, param))

    def get_params(self) -> List[str]:
        """Returns a list of parameter names."""
        return list(self.CONFIG_PARAMS)

    def get_config_dict(self) -> Dict[str, Union[str, float]]:
        """Get a dictionary representation of `SHConfig` class. If `hide_credentials` is set to `True` then
        credentials will be masked.

        :return: A dictionary with configuration parameters
        """
        config_params = {param: getattr(self, param) for param in self.CONFIG_PARAMS}

        if self._hide_credentials:
            config_params = {param: self._mask_credentials(param, value) for param, value in config_params.items()}

        return config_params

    @classmethod
    def get_config_location(cls) -> str:
        """Returns the default location of the user configuration file on disk."""
        user_folder = os.path.expanduser("~")
        return os.path.join(user_folder, ".config", "sentinelhub", "config.json")

    def _mask_credentials(self, param: str, value: object) -> object:
        """In case a parameter that holds credentials is given it will mask its value"""
        if not (param in self.CREDENTIALS and value):
            return value
        if not isinstance(value, str):
            raise ValueError(f"Parameter `{param}` should be a string but {value} found")

        hide_size = min(max(len(value) - 4, 10), len(value))
        return "*" * hide_size + value[hide_size:]

    def raise_for_missing_instance_id(self) -> None:
        """In case Sentinel Hub instance ID is missing it raises an informative error

        :raises: ValueError
        """
        if not self.instance_id:
            raise ValueError(
                "Sentinel Hub instance ID is missing. "
                "Either provide it with SHConfig object or save it into config.json configuration file. "
                "Check https://sentinelhub-py.readthedocs.io/en/latest/configure.html for more info."
            )

"""
Module for managing configuration data from `config.toml`
"""
from __future__ import annotations

import copy
import json
import numbers
import os
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple, Union

import tomli
import tomli_w

DEFAULT_PROFILE = "default-profile"
SH_PROFILE_ENV_VAR = "SH_PROFILE"
SH_CLIENT_ID_ENV_VAR = "SH_CLIENT_ID"
SH_CLIENT_SECRET_ENV_VAR = "SH_CLIENT_SECRET"


class SHConfig:  # pylint: disable=too-many-instance-attributes
    """A sentinelhub-py package configuration class.

    The class reads the configurable settings from ``config.toml`` file on initialization:

        - `instance_id`: An instance ID for Sentinel Hub service used for OGC requests.
        - `sh_client_id`: User's OAuth client ID for Sentinel Hub service. Can be set via SH_CLIENT_ID environment
          variable. The environment variable has precedence.
        - `sh_client_secret`: User's OAuth client secret for Sentinel Hub service. Can be set via SH_CLIENT_SECRET
          environment variable. The environment variable has precedence.
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

    For manual modification of `config.toml` you can see the expected location of the file via
    `SHConfig.get_config_location()`.

    Usage in the code:

        * ``SHConfig().sh_base_url``
        * ``SHConfig().instance_id``

    """

    CREDENTIALS = (
        "instance_id",
        "sh_client_id",
        "sh_client_secret",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
    )
    OTHER_PARAMS = (
        "sh_base_url",
        "sh_auth_base_url",
        "geopedia_wms_url",
        "geopedia_rest_url",
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
    )

    def __init__(self, profile: str = DEFAULT_PROFILE, *, use_defaults: bool = False):
        """
        :param profile: Specifies which profile to load form the configuration file. The environment variable
            SH_USER_PROFILE has precedence.
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

        profile = os.environ.get(SH_PROFILE_ENV_VAR, default=profile)

        if not use_defaults:
            # load from config.toml
            loaded_instance = SHConfig.load(profile=profile)  # user parameters validated in here already
            for param in SHConfig.get_params():
                setattr(self, param, getattr(loaded_instance, param))

            # check env
            self.sh_client_id = os.environ.get(SH_CLIENT_ID_ENV_VAR, default=self.sh_client_id)
            self.sh_client_secret = os.environ.get(SH_CLIENT_SECRET_ENV_VAR, default=self.sh_client_secret)

    def _validate_values(self) -> None:
        """Ensures that the values are aligned with expectations."""
        default = SHConfig(use_defaults=True)

        for param in self.get_params():
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

    def __str__(self) -> str:
        """Content of SHConfig in json schema. Credentials are masked for safety."""
        return json.dumps(self.to_dict(mask_credentials=True), indent=2)

    def __repr__(self) -> str:
        """Representation of SHConfig parameters. Credentials are masked for safety."""
        config_dict = self.to_dict(mask_credentials=True)
        content = ",\n  ".join(f"{key}={repr(value)}" for key, value in config_dict.items())
        return f"{self.__class__.__name__}(\n  {content},\n)"

    def __eq__(self, other: object) -> bool:
        """Two instances of `SHConfig` are equal if all values of their parameters are equal."""
        if not isinstance(other, SHConfig):
            return False
        return all(getattr(self, param) == getattr(other, param) for param in self.get_params())

    @classmethod
    def load(cls, filename: Optional[str] = None, profile: str = DEFAULT_PROFILE) -> SHConfig:
        """Loads configuration parameters from a file. If a filename is not specified the configuration is loaded from
        the location specified by `SHConfig.get_config_location()`.

        :param filename: Optional path of the configuration file to be loaded.
        :param profile: Which profile to load from the configuration file.
        """
        config = cls(use_defaults=True)

        if filename is None:
            filename = cls.get_config_location()
            if not os.path.exists(filename):  # nothing to load from disk
                config.save(profile)  # store default configuration to standard location
                return config

        with open(filename, "rb") as cfg_file:
            configurations_dict = tomli.load(cfg_file)

        if profile not in configurations_dict:
            raise KeyError(f"Profile {profile} not found in configuration file.")

        config_fields = cls.get_params()
        for param, value in configurations_dict[profile].items():
            if param in config_fields:
                setattr(config, param, value)

        config._validate_values()
        return config

    def save(self, filename: Optional[str] = None, profile: str = DEFAULT_PROFILE) -> None:
        """Saves configuration parameters to the user settings in the `config.toml` file.  If a filename is not
        specified, the configuration is saved to the location specified by `SHConfig.get_config_location()`.

        :param filename: Optional path of the configuration file to be saved.
        :param profile: Under which profile to save the configuration.
        """
        self._validate_values()

        file_path = Path(filename or self.get_config_location())
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if file_path.exists():
            with open(file_path, "rb") as cfg_file:
                current_configuration = tomli.load(cfg_file)
        else:
            current_configuration = {}

        current_configuration[profile] = self._get_dict_of_diffs_from_defaults()
        with open(file_path, "wb") as cfg_file:
            tomli_w.dump(current_configuration, cfg_file)

    def _get_dict_of_diffs_from_defaults(self) -> Dict[str, Union[str, float]]:
        """Returns a dictionary containing key: value pairs for parameters that have values different from defaults."""
        current_profile_config = self.to_dict(mask_credentials=False)
        default_values = SHConfig(use_defaults=True).to_dict(mask_credentials=False)
        return {key: value for key, value in current_profile_config.items() if default_values[key] != value}

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

    @classmethod
    def get_params(cls) -> Tuple[str, ...]:
        """Returns a list of parameter names."""
        return cls.CREDENTIALS + cls.OTHER_PARAMS

    def to_dict(self, mask_credentials: bool = True) -> Dict[str, Union[str, float]]:
        """Get a dictionary representation of the `SHConfig` class.

        :param hide_credentials: Wether to mask fields containing credentials.
        :return: A dictionary with configuration parameters
        """
        config_params = {param: getattr(self, param) for param in self.get_params()}

        if mask_credentials:
            for param in self.CREDENTIALS:
                config_params[param] = self._mask_credentials(config_params[param])

        return config_params

    def _mask_credentials(self, value: str) -> str:
        """In case a parameter that holds credentials is given it will mask its value"""
        hide_size = min(max(len(value) - 4, 10), len(value))
        return "*" * hide_size + value[hide_size:]

    @classmethod
    def get_config_location(cls) -> str:
        """Returns the default location of the user configuration file on disk."""
        user_folder = os.path.expanduser("~")
        return os.path.join(user_folder, ".config", "sentinelhub", "config.toml")

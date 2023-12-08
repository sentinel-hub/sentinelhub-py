"""
Module for managing configuration data from `config.toml`
"""

from __future__ import annotations

import copy
import json
import os
import warnings
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import tomli
import tomli_w

from .exceptions import SHDeprecationWarning

DEFAULT_PROFILE = "default-profile"
SH_PROFILE_ENV_VAR = "SH_PROFILE"
SH_CLIENT_ID_ENV_VAR = "SH_CLIENT_ID"
SH_CLIENT_SECRET_ENV_VAR = "SH_CLIENT_SECRET"


@dataclass(repr=False)
class _SHConfig:
    instance_id: str = ""
    sh_client_id: str = ""
    sh_client_secret: str = ""
    sh_base_url: str = "https://services.sentinel-hub.com"
    sh_auth_base_url: str | None = None
    sh_token_url: str = "https://services.sentinel-hub.com/auth/realms/main/protocol/openid-connect/token"
    geopedia_wms_url: str = "https://service.geopedia.world"
    geopedia_rest_url: str = "https://www.geopedia.world/rest"
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_session_token: str = ""
    aws_metadata_url: str = "https://roda.sentinel-hub.com"
    aws_s3_l1c_bucket: str = "sentinel-s2-l1c"
    aws_s3_l2a_bucket: str = "sentinel-s2-l2a"
    opensearch_url: str = "http://opensearch.sentinel-hub.com/resto/api/collections/Sentinel2"
    max_wfs_records_per_query: int = 100
    max_opensearch_records_per_query: int = 500  # pylint: disable=invalid-name
    max_download_attempts: int = 4
    download_sleep_time: float = 5.0
    download_timeout_seconds: float = 120.0
    number_of_download_processes: int = 1

    def __post_init__(self) -> None:
        if self.sh_auth_base_url is not None:
            self.sh_token_url = self.sh_auth_base_url + "/oauth/token"
            warnings.warn(
                "The parameter `sh_auth_base_url` of `SHConfig` has been replaced with `sh_token_url`. Please"
                " update your configuration, for now the parameters were automatically adjusted to `sh_token_url ="
                " sh_auth_base_url + '/oauth/token'`.",
                category=SHDeprecationWarning,
            )

        if self.max_wfs_records_per_query > 100:
            raise ValueError("Value of config parameter `max_wfs_records_per_query` must be at most 100")
        if self.max_opensearch_records_per_query > 500:
            raise ValueError("Value of config parameter `max_opensearch_records_per_query` must be at most 500")


class SHConfig(_SHConfig):
    """A sentinelhub-py package configuration class.

    The class reads the configurable settings from ``config.toml`` file on initialization:

        - `instance_id`: An instance ID for Sentinel Hub service used for OGC requests.
        - `sh_client_id`: User's OAuth client ID for Sentinel Hub service. Can be set via SH_CLIENT_ID environment
          variable. The environment variable has precedence.
        - `sh_client_secret`: User's OAuth client secret for Sentinel Hub service. Can be set via SH_CLIENT_SECRET
          environment variable. The environment variable has precedence.
        - `sh_base_url`: There exist multiple deployed instances of Sentinel Hub service, this parameter defines the
          location of a specific service instance.
        - `sh_token_url`: Url for Sentinel Hub Authentication service. Authentication is typically sent to the main
          service deployment even if `sh_base_url` points to another deployment.
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

    The location of `config.toml` for manual modification can be found with `SHConfig.get_config_location()`.
    """

    CREDENTIALS = (
        "instance_id",
        "sh_client_id",
        "sh_client_secret",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
    )

    def __init__(self, profile: str | None = None, *, use_defaults: bool = False, **kwargs: Any):
        """
        :param profile: Specifies which profile to load from the configuration file. Has precedence over the environment
            variable `SH_USER_PROFILE`.
        :param use_defaults: Does not load the configuration file, returns config object with defaults only.
        :param kwargs: Any fields of `SHConfig` to be updated. Overrides settings from `config.toml` and environment.
        """
        profile = self._get_profile(profile)

        if not use_defaults:
            env_kwargs = {
                "sh_client_id": os.environ.get(SH_CLIENT_ID_ENV_VAR),
                "sh_client_secret": os.environ.get(SH_CLIENT_SECRET_ENV_VAR),
            }
            env_kwargs = {k: v for k, v in env_kwargs.items() if v is not None}

            # load from config.toml
            loaded_kwargs = SHConfig.load(profile=profile).to_dict(mask_credentials=False)

            kwargs = {**loaded_kwargs, **env_kwargs, **kwargs}  # precedence: init params > env > loaded

        super().__init__(**kwargs)

    def __str__(self) -> str:
        """Content of `SHConfig` in json schema. Credentials are masked for safety."""
        return json.dumps(self.to_dict(mask_credentials=True), indent=2)

    def __repr__(self) -> str:
        """Representation of `SHConfig`. Credentials are masked for safety."""
        config_dict = self.to_dict(mask_credentials=True)
        content = ",\n  ".join(f"{key}={value!r}" for key, value in config_dict.items())
        return f"{self.__class__.__name__}(\n  {content},\n)"

    @staticmethod
    def _get_profile(profile: str | None) -> str:
        return profile if profile is not None else os.environ.get(SH_PROFILE_ENV_VAR, default=DEFAULT_PROFILE)

    @classmethod
    def load(cls, profile: str | None = None) -> SHConfig:
        """Loads configuration parameters from the config file at `SHConfig.get_config_location()`.

        :param profile: Which profile to load from the configuration file.
        """
        profile = cls._get_profile(profile)
        filename = cls.get_config_location()
        if not os.path.exists(filename):
            cls(use_defaults=True).save()  # store default configuration to standard location

        with open(filename, "rb") as cfg_file:
            configurations_dict = tomli.load(cfg_file)

        if profile not in configurations_dict:
            raise KeyError(f"Profile `{profile}` not found in configuration file.")

        return cls(use_defaults=True, **configurations_dict[profile])

    def save(self, profile: str | None = None) -> None:
        """Saves configuration parameters to the config file at `SHConfig.get_config_location()`.

        :param profile: Under which profile to save the configuration.
        """
        profile = self._get_profile(profile)
        file_path = Path(self.get_config_location())
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if file_path.exists():
            with open(file_path, "rb") as cfg_file:
                current_configuration = tomli.load(cfg_file)
        else:
            current_configuration = {}

        current_configuration[profile] = self._get_dict_of_diffs_from_defaults()
        with open(file_path, "wb") as cfg_file:
            tomli_w.dump(current_configuration, cfg_file)

    def _get_dict_of_diffs_from_defaults(self) -> dict[str, str | float]:
        """Returns a dictionary containing key: value pairs for parameters that have values different from defaults."""
        current_profile_config = self.to_dict(mask_credentials=False)
        default_values = SHConfig(use_defaults=True).to_dict(mask_credentials=False)
        return {key: value for key, value in current_profile_config.items() if default_values[key] != value}

    def copy(self) -> SHConfig:
        """Makes a copy of an instance of `SHConfig`"""
        return copy.copy(self)

    def to_dict(self, mask_credentials: bool = True) -> dict[str, str | float]:
        """Get a dictionary representation of the `SHConfig` class.

        :param mask_credentials: Wether to mask fields containing credentials.
        :return: A dictionary with configuration parameters
        """
        config_params = asdict(self)

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

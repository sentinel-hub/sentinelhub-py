"""
Module implementing Sentinel Hub session object
"""
import base64
import json
import logging
import time
from typing import Any, Dict, Optional

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from ..config import SHConfig
from ..download.handlers import fail_user_errors, retry_temporary_errors
from ..download.request import DownloadRequest

LOGGER = logging.getLogger(__name__)


class SentinelHubSession:
    """Sentinel Hub authentication class

    The class will do OAuth2 authentication with Sentinel Hub service and store the token. It will make sure that the
    token is never expired by automatically refreshing it if expiry time is close.

    For more info about Sentinel Hub authentication check
    `service documentation <https://docs.sentinel-hub.com/api/latest/api/overview/authentication/>`__.
    """

    SECONDS_BEFORE_EXPIRY = 60

    def __init__(self, config: Optional[SHConfig] = None):
        """
        :param config: An instance of package configuration class
        """
        self.config = config or SHConfig()

        if not (self.config.sh_client_id and self.config.sh_client_secret):
            raise ValueError(
                "Configuration parameters 'sh_client_id' and 'sh_client_secret' have to be set in order "
                "to authenticate with Sentinel Hub service. Check "
                "https://sentinelhub-py.readthedocs.io/en/latest/configure.html for more info."
            )

        self._token = self._collect_new_token()

    @property
    def token(self) -> Dict[str, Any]:
        """Always up-to-date session's token

        :return: A token in a form of dictionary of parameters
        """
        if self._token and self._token["expires_at"] > time.time() + self.SECONDS_BEFORE_EXPIRY:
            return self._token

        self._token = self._collect_new_token()
        return self._token

    def info(self) -> Dict[str, Any]:
        """Decode token to get token info"""

        token = self.token["access_token"].split(".")[1]
        padded = token + "=" * (len(token) % 4)
        decoded_string = base64.b64decode(padded).decode()
        return json.loads(decoded_string)

    @property
    def session_headers(self) -> Dict[str, str]:
        """Provides session authorization headers

        :return: A dictionary with authorization headers.
        """
        return {"Authorization": f'Bearer {self.token["access_token"]}'}

    def _collect_new_token(self) -> Dict[str, Any]:
        """Creates a download request and fetches a token from the service.

        Note that the `DownloadRequest` object is created only because retry decorators of `_fetch_method` require it.
        """
        request = DownloadRequest(url=self.config.get_sh_oauth_url())
        return self._fetch_token(request)

    @retry_temporary_errors
    @fail_user_errors
    def _fetch_token(self, request: DownloadRequest) -> Dict[str, Any]:
        """Collects a new token from Sentinel Hub service"""
        oauth_client = BackendApplicationClient(client_id=self.config.sh_client_id)

        LOGGER.debug("Creating a new authentication session with Sentinel Hub service")
        with OAuth2Session(client=oauth_client) as oauth_session:
            return oauth_session.fetch_token(
                token_url=request.url, client_id=self.config.sh_client_id, client_secret=self.config.sh_client_secret
            )

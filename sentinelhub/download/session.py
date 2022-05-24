"""
Module implementing Sentinel Hub session object
"""
import base64
import json
import logging
import time
import warnings
from typing import Any, Dict, Optional

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from ..config import SHConfig
from ..download.handlers import fail_user_errors, retry_temporary_errors
from ..download.request import DownloadRequest
from ..exceptions import SHUserWarning

LOGGER = logging.getLogger(__name__)

JsonDict = Dict[str, Any]


class SentinelHubSession:
    """Sentinel Hub authentication class

    The class will do OAuth2 authentication with Sentinel Hub service and store the token. It is able to refresh the
    token before it expires and decode user information from the token.

    For more info about Sentinel Hub authentication check
    `service documentation <https://docs.sentinel-hub.com/api/latest/api/overview/authentication/>`__.
    """

    DEFAULT_SECONDS_BEFORE_EXPIRY = 60

    def __init__(
        self,
        config: Optional[SHConfig] = None,
        refresh_before_expiry: Optional[float] = DEFAULT_SECONDS_BEFORE_EXPIRY,
        *,
        _token: Optional[JsonDict] = None,
    ):
        """
        :param config: A config object containing Sentinel Hub OAuth credentials and the base URL of the service.
        :param refresh_before_expiry: A number of seconds before authentication token expiry at which time a refreshing
            mechanism is activated. When this is activated it means that whenever a valid token will be again
            required the `SentinelHubSession` will re-authenticate to Sentinel Hub service and obtain a new token.
            By default, the parameter is set to `60` seconds. If this parameter is set to `None` it will deactivate
            token refreshing and `SentinelHubSession` might provide a token that is already expired. This can be used
            to avoid re-authenticating too many times.
        """
        self.config = config or SHConfig()
        self.refresh_before_expiry = refresh_before_expiry

        token_fetching_required = _token is None or self.refresh_before_expiry is not None
        if token_fetching_required and not (self.config.sh_client_id and self.config.sh_client_secret):
            raise ValueError(
                "Configuration parameters 'sh_client_id' and 'sh_client_secret' have to be set in order "
                "to authenticate with Sentinel Hub service. Check "
                "https://sentinelhub-py.readthedocs.io/en/latest/configure.html for more info."
            )

        self._token = self._collect_new_token() if _token is None else _token

    @classmethod
    def from_token(cls, token: Dict[str, Any]) -> "SentinelHubSession":
        """Create a session object from the given token. The created session is configured not to refresh its token.

        :param token: A dictionary containing token object.
        """
        for key in ["access_token", "expires_at"]:
            if key not in token:
                raise ValueError(f"Given token should be a dictionary containing a key '{key}'")

        return cls(_token=token, refresh_before_expiry=None)

    @property
    def token(self) -> JsonDict:
        """Always up-to-date session's token

        :return: A token in a form of dictionary of parameters
        """
        remaining_token_time = self._token["expires_at"] - time.time()
        if self.refresh_before_expiry is None:
            if remaining_token_time <= 0:
                warnings.warn("The Sentinel Hub session token seems to be expired.", category=SHUserWarning)
            return self._token

        if remaining_token_time <= self.refresh_before_expiry:
            self._token = self._collect_new_token()

        return self._token

    def info(self) -> JsonDict:
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

    def _collect_new_token(self) -> JsonDict:
        """Creates a download request and fetches a token from the service.

        Note that the `DownloadRequest` object is created only because retry decorators of `_fetch_token` method
        require it.
        """
        request = DownloadRequest(url=self.config.get_sh_oauth_url())
        return self._fetch_token(request)

    @retry_temporary_errors
    @fail_user_errors
    def _fetch_token(self, request: DownloadRequest) -> JsonDict:
        """Collects a new token from Sentinel Hub service"""
        oauth_client = BackendApplicationClient(client_id=self.config.sh_client_id)

        LOGGER.debug("Creating a new authentication session with Sentinel Hub service")
        with OAuth2Session(client=oauth_client) as oauth_session:
            return oauth_session.fetch_token(
                token_url=request.url, client_id=self.config.sh_client_id, client_secret=self.config.sh_client_secret
            )

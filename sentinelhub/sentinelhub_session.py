"""
Module implementing Sentinel Hub session object
"""
import logging
import time

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from .config import SHConfig
from .download.request import DownloadRequest
from .download.handlers import retry_temporary_errors, fail_user_errors


LOGGER = logging.getLogger(__name__)


class SentinelHubSession:
    """ Sentinel Hub authentication class

    The class will do OAuth2 authentication with Sentinel Hub service and store the token. It will make sure that the
    token is never expired by automatically refreshing it if expiry time is close.
    """
    SECONDS_BEFORE_EXPIRY = 60

    def __init__(self, config=None):
        """
        :param config: An instance of package configuration class
        :type config: SHConfig
        """
        self.config = config or SHConfig()

        if not (self.config.sh_client_id and self.config.sh_client_secret):
            raise ValueError("Configuration parameters 'sh_client_id' and 'sh_client_secret' have to be set in order "
                             "to authenticate with Sentinel Hub service. Check "
                             "http://sentinelhub-py.readthedocs.io/en/latest/configure.html for more info.")

        self._token = None
        _ = self.token

    @property
    def token(self):
        """ Always up-to-date session's token

        :return: A token in a form of dictionary of parameters
        :rtype: dict
        """
        if self._token and self._token['expires_at'] > time.time() + self.SECONDS_BEFORE_EXPIRY:
            return self._token

        # A request parameter is created only in order for error handling decorators to work correctly
        request = DownloadRequest(url=self.config.get_sh_oauth_url())
        self._token = self._fetch_token(request)

        return self._token

    @property
    def session_headers(self):
        """ Provides session authorization headers

        :return: A dictionary with authorization headers
        :rtype: dict
        """
        return {
            'Authorization': f'Bearer {self.token["access_token"]}'
        }

    @retry_temporary_errors
    @fail_user_errors
    def _fetch_token(self, request):
        """ Collects a new token from Sentinel Hub service
        """
        oauth_client = BackendApplicationClient(client_id=self.config.sh_client_id)

        LOGGER.debug('Creating a new authentication session with Sentinel Hub service')
        with OAuth2Session(client=oauth_client) as oauth_session:
            return oauth_session.fetch_token(
                token_url=request.url,
                client_id=self.config.sh_client_id,
                client_secret=self.config.sh_client_secret
            )

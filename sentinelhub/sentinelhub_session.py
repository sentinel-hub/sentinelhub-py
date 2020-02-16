"""
Module implementing Sentinel Hub session object
"""
import time

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from .config import SHConfig


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
        self.config = SHConfig() if config is None else config

        if not (self.config.sh_client_id and self.config.sh_client_secret):
            raise ValueError("Configuration parameters 'sh_client_id' and 'sh_client_secret' have to be set in order"
                             "to authenticate with Sentinel Hub service")

        self._token = None

    @property
    def token(self):
        """ Always up-to-date session's token

        :return: A token in a form of dictionary of parameters
        :rtype: dict
        """
        if self._token and self._token['expires_at'] > time.time() + self.SECONDS_BEFORE_EXPIRY:
            return self._token

        self._token = self._fetch_token()
        return self._token

    @property
    def session_headers(self):
        """ Provides session authorization headers

        :return: A dictionary with authorization headers
        :rtype: dict
        """
        return {
            'Authorization': 'Bearer {}'.format(self.token['access_token'])
        }

    def _fetch_token(self):
        """ Collects a new token from Sentinel Hub service
        """
        oauth_client = BackendApplicationClient(client_id=self.config.sh_client_id)

        with OAuth2Session(client=oauth_client) as oauth_session:
            return oauth_session.fetch_token(
                token_url=self.config.get_sh_oauth_url(),
                client_id=self.config.sh_client_id,
                client_secret=self.config.sh_client_secret
            )

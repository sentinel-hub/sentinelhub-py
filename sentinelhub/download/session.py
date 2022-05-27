"""
Module implementing Sentinel Hub session object
"""
import base64
import json
import logging
import sys
import time
import warnings
from threading import Event, Thread
from typing import Any, Dict, Optional

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from ..config import SHConfig
from ..download.handlers import fail_user_errors, retry_temporary_errors
from ..download.request import DownloadRequest
from ..exceptions import SHUserWarning
from ..type_utils import JsonDict

if sys.version_info < (3, 8):
    from shared_memory import SharedMemory
else:
    from multiprocessing.shared_memory import SharedMemory


LOGGER = logging.getLogger(__name__)


class SentinelHubSession:
    """Sentinel Hub authentication class

    The class will do OAuth2 authentication with Sentinel Hub service and store the token. It is able to refresh the
    token before it expires and decode user information from the token.

    For more info about Sentinel Hub authentication check
    `service documentation <https://docs.sentinel-hub.com/api/latest/api/overview/authentication/>`__.
    """

    DEFAULT_SECONDS_BEFORE_EXPIRY = 120

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
    def from_token(cls, token: JsonDict) -> "SentinelHubSession":
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


_DEFAULT_SESSION_MEMORY_NAME = "sh-session-token"
_NULL_MEMORY_VALUE = b"\x00"


class SessionSharingThread(Thread):
    """A thread for sharing a token from `SentinelHubSession` object in a shared memory object that can be accessed by
    other Python processes during multiprocessing parallelization.

    How to use it:

    .. code-block:: python

        thread = SessionSharingThread(session)
        thread.start()

        # Run a parallelization process here
        # Use collect_shared_session() to retrieve the session with other processes

        thread.join()
    """

    _EXTRA_MEMORY_BYTES = 100

    def __init__(self, session: SentinelHubSession, memory_name: str = _DEFAULT_SESSION_MEMORY_NAME, **kwargs: Any):
        """
        :param session: A Sentinel Hub session to be used for sharing its authentication token.
        :param memory_name: A unique name for the requested shared memory block.
        :param kwargs: Keyword arguments to be propagated to `threading.Thread` parent class.
        """
        super().__init__(**kwargs)

        self.session = session
        self.memory_name = memory_name

        if self.session.refresh_before_expiry is None:
            raise ValueError(f"Given instance of {self.session.__class__.__name__} must be self-refreshing")
        self._refresh_time = self.session.refresh_before_expiry

        self._stop_event = Event()
        self._is_memory_shared_event = Event()

    def start(self) -> None:
        """Start running the thread.

        After starting the thread it also waits for the token to be shared. This way no other process would try to
        access the memory before it even exists."""
        super().start()
        self._is_memory_shared_event.wait()

    def run(self) -> None:
        """A running thread is running an infinite loop of sharing a token and waiting for token to expire. The loop
        ends only when the thread is stopped."""
        self._stop_event.clear()

        while not self._stop_event.is_set():
            token = self.session.token
            self._share_token(token)

            sleep_until_refresh_time = token["expires_at"] - time.time() - self._refresh_time
            if sleep_until_refresh_time > 0:
                self._stop_event.wait(timeout=sleep_until_refresh_time)

    def _share_token(self, token: JsonDict) -> None:
        """A token is encoded into bytes and written into a shared memory block.

        Note that the `SharedMemory` object allocates extra `self._EXTRA_MEMORY_BYTES` bytes of memory because the
        length of encoded token can vary a bit.
        """
        encoded_token = json.dumps(token).encode()

        if self._is_memory_shared_event.is_set():
            memory = SharedMemory(name=self.memory_name)
        else:
            memory = SharedMemory(
                create=True,
                size=len(encoded_token) + self._EXTRA_MEMORY_BYTES,
                name=self.memory_name,
            )
            self._is_memory_shared_event.set()

        try:
            memory.buf[:] = encoded_token + _NULL_MEMORY_VALUE * (memory.size - len(encoded_token))
        finally:
            memory.close()

    def join(self, timeout: Optional[float] = None) -> None:
        """The method stops the thread that would otherwise run indefinitely and joins it with the main thread.

        :param timeout: Parameter that is propagated to `threading.Thread.join` method.
        """
        self._stop_event.set()
        super().join(timeout=timeout)

        if self._is_memory_shared_event.is_set():
            memory = SharedMemory(name=self.memory_name)
            memory.unlink()
            self._is_memory_shared_event.clear()
            memory.close()


def collect_shared_session(memory_name: str = _DEFAULT_SESSION_MEMORY_NAME) -> SentinelHubSession:
    """This utility function is meant to be used in combination with `SessionSharingThread`. It retrieves an
    authentication token from the shared memory and returns it in an `SentinelHubSession` object.

    :param memory_name: A unique name of the requested shared memory block from where to read the session. It should
        match the one used in `SessionSharingThread`.
    :return: An instance of `SentinelHubSession` that contains the shared token but is not self-refreshing.
    """
    memory = SharedMemory(name=memory_name)
    try:
        encoded_token = memory.buf.tobytes().rstrip(_NULL_MEMORY_VALUE)
    finally:
        memory.close()

    token: JsonDict = json.loads(encoded_token)
    return SentinelHubSession.from_token(token)

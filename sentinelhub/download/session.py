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

import requests
from oauthlib.oauth2 import BackendApplicationClient
from requests import Response
from requests.exceptions import JSONDecodeError
from requests_oauthlib import OAuth2Session

from ..config import SHConfig
from ..constants import SHConstants
from ..download.handlers import fail_user_errors, retry_temporary_errors
from ..download.models import DownloadRequest
from ..exceptions import SHUserWarning
from ..types import JsonDict

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
    DEFAULT_HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}  # Following SH API documentation

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
                raise ValueError(f"Given token should be a dictionary containing a key `{key}`")

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
            oauth_session.register_compliance_hook("access_token_response", self._compliance_hook)

            return oauth_session.fetch_token(
                token_url=request.url,
                client_id=self.config.sh_client_id,
                client_secret=self.config.sh_client_secret,
                headers={**self.DEFAULT_HEADERS, **SHConstants.HEADERS},
            )

    @staticmethod
    def _compliance_hook(response: Response) -> Response:
        """Checks if a response from Sentinel Hub Authentication service has an error status code but no error message.

        By default, `requests_oauthlib` ignores status of a response and only looks at an error message in a
        response body. However, Sentinel Hub service can return a response with an error status code and
        without an error message. In such cases `requests_oauthlib` would raise a completely wrong error message. This
        hook makes sure that a correct error message is raised.

        It is important that in case of 5xx errors an error is always raised so that authentication can be retried.
        But in case of 4xx errors where response contains an error message this method intentionally doesn't raise
        an error so that `oauthlib` can later raise a more descriptive error.
        """
        if response.status_code >= requests.status_codes.codes.INTERNAL_SERVER_ERROR:
            response.raise_for_status()

        try:
            token_dict = response.json()
            if "error" in token_dict:
                return response
        except JSONDecodeError:
            pass

        response.raise_for_status()
        return response


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
        """A token is encoded into bytes and written into a shared memory block."""
        encoded_token = json.dumps(token).encode()
        memory = self._get_shared_memory(encoded_token)

        try:
            memory.buf[:] = encoded_token + _NULL_MEMORY_VALUE * (memory.size - len(encoded_token))
        finally:
            memory.close()

    def _get_shared_memory(self, encoded_token: bytes) -> SharedMemory:
        """Provides a shared memory object.

        The method also handles a case where a shared memory with the same name would be left unclosed from before.
        Because the memory can be persistent and requires low-level knowledge of `multiprocessing.shared_memory` to
        close it manually this method will close it automatically and inform users about the problem.
        """
        if self._is_memory_shared_event.is_set():
            return SharedMemory(name=self.memory_name)

        try:
            memory = self._create_shared_memory(encoded_token)
        except FileExistsError:
            warnings.warn(
                (
                    f"A shared memory with a name `{self.memory_name}` already exists. It will be removed and allocated"
                    f" anew. Please make sure that every {self.__class__.__name__} instance is joined at the end. If"
                    " you are using multiple threads then specify different 'memory_name' parameter for each of them."
                ),
                category=SHUserWarning,
            )

            memory = SharedMemory(name=self.memory_name)
            memory.unlink()
            memory.close()

            memory = self._create_shared_memory(encoded_token)

        self._is_memory_shared_event.set()
        return memory

    def _create_shared_memory(self, encoded_token: bytes) -> SharedMemory:
        """Create a new shared memory space.

        Note that the `SharedMemory` object allocates extra `self._EXTRA_MEMORY_BYTES` bytes of memory because the
        length of encoded token can vary a bit.
        """
        return SharedMemory(
            create=True,
            size=len(encoded_token) + self._EXTRA_MEMORY_BYTES,
            name=self.memory_name,
        )

    def join(self, timeout: Optional[float] = None) -> None:
        """The method stops the thread that would otherwise run indefinitely and joins it with the main thread.

        :param timeout: Parameter that is propagated to `threading.Thread.join` method.
        """
        self._stop_event.set()
        super().join(timeout=timeout)

        if self._is_memory_shared_event.is_set():
            try:
                memory = SharedMemory(name=self.memory_name)
                memory.unlink()
                memory.close()
            except FileNotFoundError:
                pass

            self._is_memory_shared_event.clear()


class SessionSharing:
    """An object that in the background runs a `SessionSharingThread` which shares a Sentinel Hub authentication
    token in a shared memory object that can be accessed by other Python processes during multiprocessing
    parallelization. The object also makes sure that the thread is always closed at the end.

    How to use it:

    .. code-block:: python

        with SessionSharing(session):
            # Run a parallelization process here
    """

    def __init__(self, session: SentinelHubSession, **kwargs: Any):
        """
        :param args: A Sentinel Hub session to be used for sharing its authentication token.
        :param kwargs: Keyword arguments to be propagated to `SessionSharingThread`.
        """
        self.thread = SessionSharingThread(session, **kwargs)

    def __enter__(self) -> None:
        """Starts running the session-sharing thread."""
        self.thread.start()

    def __exit__(self, *_: Any, **__: Any) -> None:
        """Closes the running session-sharing thread."""
        self.thread.join()


def collect_shared_session(memory_name: str = _DEFAULT_SESSION_MEMORY_NAME) -> SentinelHubSession:
    """This utility function is meant to be used in combination with `SessionSharingThread`. It retrieves an
    authentication token from the shared memory and returns it in an `SentinelHubSession` object.

    :param memory_name: A unique name of the requested shared memory block from where to read the session. It should
        match the one used in `SessionSharingThread`.
    :return: An instance of `SentinelHubSession` that contains the shared token but is not self-refreshing.
    """
    try:
        memory = SharedMemory(name=memory_name)
    except FileNotFoundError as exception:
        raise FileNotFoundError(
            f"Couldn't obtain a shared session because a shared memory `{memory_name}` doesn't exist. Make sure that"
            " you are running session sharing when calling this function"
        ) from exception

    try:
        encoded_token = memory.buf.tobytes().rstrip(_NULL_MEMORY_VALUE)
    finally:
        memory.close()

    token: JsonDict = json.loads(encoded_token)
    return SentinelHubSession.from_token(token)

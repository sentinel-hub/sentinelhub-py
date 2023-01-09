"""
Module implementing a rate-limited multithreaded download client for downloading from Sentinel Hub service
"""
import logging
import time
import warnings
from threading import Lock
from typing import Any, Callable, Dict, Optional, Tuple, TypeVar, Union

import requests
from requests import Response

from ..config import SHConfig
from ..constants import SHConstants
from ..exceptions import SHRateLimitWarning, SHRuntimeWarning
from ..types import JsonDict
from .client import DownloadClient
from .handlers import fail_user_errors, retry_temporary_errors
from .models import DownloadRequest, DownloadResponse
from .rate_limit import SentinelHubRateLimit
from .session import SentinelHubSession

LOGGER = logging.getLogger(__name__)

T = TypeVar("T")


class SentinelHubDownloadClient(DownloadClient):
    """Download client specifically configured for download from Sentinel Hub service"""

    _CACHED_SESSIONS: Dict[Tuple[str, str], SentinelHubSession] = {}
    _UNIVERSAL_CACHE_KEY = "universal-user", "default-url"

    def __init__(self, *, session: Optional[SentinelHubSession] = None, **kwargs: Any):
        """
        :param session: If a session object is provided here then this client instance will always use only the
            provided session. Otherwise, it will either use a cached session or create a new session and cache
            it.
        :param kwargs: Optional parameters from DownloadClient
        """
        super().__init__(**kwargs)

        if session is not None and not isinstance(session, SentinelHubSession):
            raise ValueError(
                f"A session parameter has to be an instance of {SentinelHubSession.__name__} or None, but "
                f"{session} was given"
            )
        self.session = session

        self.rate_limit = SentinelHubRateLimit(num_processes=self.config.number_of_download_processes)
        self.lock: Optional[Lock] = None

    def download(self, *args: Any, **kwargs: Any) -> Any:
        """The main download method

        :param args: Passed to `DownloadClient.download`
        :param kwargs: Passed to `DownloadClient.download`
        """
        # Because the Lock object cannot be pickled we create it only here and remove it afterward
        self.lock = Lock()
        try:
            return super().download(*args, **kwargs)
        finally:
            self.lock = None

    @retry_temporary_errors
    @fail_user_errors
    def _execute_download(self, request: DownloadRequest) -> DownloadResponse:
        """
        Executes the download with a single thread and uses a rate limit object, which is shared between all threads
        """
        while True:
            sleep_time = self._execute_thread_safe(self.rate_limit.register_next)

            if sleep_time == 0:
                LOGGER.debug(
                    "Sending %s request to %s. Hash of sent request is %s",
                    request.request_type.value,
                    request.url,
                    request.get_hashed_name(),
                )
                response = self._do_download(request)

                self._execute_thread_safe(self.rate_limit.update, response.headers)

                if response.status_code == requests.status_codes.codes.TOO_MANY_REQUESTS:
                    warnings.warn("Download rate limit hit", category=SHRateLimitWarning)
                    continue

                response.raise_for_status()

                LOGGER.debug("Successful %s request to %s", request.request_type.value, request.url)
                return DownloadResponse.from_response(response, request)

            LOGGER.debug("Request needs to wait. Sleeping for %0.2f", sleep_time)
            time.sleep(sleep_time)

    def _execute_thread_safe(self, thread_unsafe_function: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Executes a function inside a thread lock and handles potential errors"""
        if self.lock is None:
            return thread_unsafe_function(*args, **kwargs)

        with self.lock:
            return thread_unsafe_function(*args, **kwargs)

    def _do_download(self, request: DownloadRequest) -> Response:
        """Runs the download"""
        if request.url is None:
            raise ValueError(f"Faulty request {request}, no URL specified.")

        return requests.request(
            request.request_type.value,
            url=request.url,
            json=request.post_values,
            headers=self._prepare_headers(request),
            timeout=self.config.download_timeout_seconds,
        )

    def _prepare_headers(self, request: DownloadRequest) -> JsonDict:
        """Prepares final headers by potentially joining them with session headers. Note that in the current
        implementation of this method request headers have priority to overwrite default and session headers with the
        same keys.
        """
        session_headers: JsonDict = {}
        if request.use_session:
            session_headers = self._execute_thread_safe(self._get_session_headers)

        return {**SHConstants.HEADERS, **session_headers, **request.headers}

    def _get_session_headers(self) -> JsonDict:
        """Provides up-to-date session headers

        Note that calling session_headers property triggers update if session has expired therefore this has to be
        called in a thread-safe way
        """
        return self.get_session().session_headers

    def get_session(self) -> SentinelHubSession:
        """Provides the session object used by the client

        :return: A Sentinel Hub session object
        """
        if self.session:
            return self.session

        cache_key = self._get_cache_key(self.config)
        if cache_key in SentinelHubDownloadClient._CACHED_SESSIONS:
            session = SentinelHubDownloadClient._CACHED_SESSIONS[cache_key]
        elif SentinelHubDownloadClient._UNIVERSAL_CACHE_KEY in SentinelHubDownloadClient._CACHED_SESSIONS:
            session = SentinelHubDownloadClient._CACHED_SESSIONS[SentinelHubDownloadClient._UNIVERSAL_CACHE_KEY]
        else:
            session = SentinelHubSession(config=self.config)
            SentinelHubDownloadClient._CACHED_SESSIONS[cache_key] = session

        return session

    @staticmethod
    def cache_session(session: SentinelHubSession, universal: bool = False) -> None:
        """Cache a Sentinel Hub session for to be reused by all instances of `SentinelHubDownloadClient` and its child
        classes within the same Python runtime environment.

        :param session: A session object to be cached.
        :param universal: By default a session is cached for a specific OAuth user ID and Sentinel Hub deployment. But
            if this flag is set to `True` it will cache session for any OAuth user ID and deployment. The intended
            purpose of this parameter is that when a session is sent to a remote processing instance, which doesn't
            have configured Sentinel Hub OAuth credentials, then the session can still be used even without credentials.
        """
        if not isinstance(session, SentinelHubSession):
            raise ValueError(
                f"Given object should be an instance of {SentinelHubSession.__name__} but {session} was given"
            )

        cache_key = (
            SentinelHubDownloadClient._UNIVERSAL_CACHE_KEY
            if universal
            else SentinelHubDownloadClient._get_cache_key(session)
        )
        SentinelHubDownloadClient._CACHED_SESSIONS[cache_key] = session

    @staticmethod
    def _get_cache_key(config_or_session: Union[SentinelHubSession, SHConfig]) -> Tuple[str, str]:
        """Calculates a cache key for the given session or config object. The key consists of an OAuth client ID and
        a base service URL.
        """
        if isinstance(config_or_session, SHConfig):
            return config_or_session.sh_client_id, config_or_session.sh_base_url

        if isinstance(config_or_session, SentinelHubSession):
            base_url = config_or_session.config.sh_base_url

            # If session was generated from token then config_or_session.config.sh_client_id could have wrong client id.
            sh_client_id = config_or_session.info().get("aud", "")
            if not sh_client_id:
                warnings.warn(
                    "Failed to read client ID from OAuth token. Session caching might not work correctly.",
                    category=SHRuntimeWarning,
                )

            return sh_client_id, base_url

        raise ValueError(f"Expected a config or a session object but got {config_or_session}")

    @staticmethod
    def clear_cache() -> None:
        """Clears cached sessions."""
        SentinelHubDownloadClient._CACHED_SESSIONS = {}

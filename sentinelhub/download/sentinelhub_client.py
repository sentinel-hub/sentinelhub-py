"""
Module implementing a rate-limited multithreaded download client for downloading from Sentinel Hub service
"""
import logging
import time
from typing import Any, Callable, Dict, Optional, Tuple, TypeVar
import warnings
from threading import Lock

import requests

from sentinelhub.download.request import DownloadRequest

from ..exceptions import SHRateLimitWarning
from .client import DownloadClient
from .handlers import fail_user_errors, retry_temporary_errors
from .rate_limit import SentinelHubRateLimit
from .session import SentinelHubSession

LOGGER = logging.getLogger(__name__)

T = TypeVar("T")

class SentinelHubDownloadClient(DownloadClient):
    """Download client specifically configured for download from Sentinel Hub service"""

    _CACHED_SESSIONS: Dict[Tuple[str, str, str], SentinelHubSession] = {}

    def __init__(self, *, session: Optional[SentinelHubSession] = None, **kwargs: Any):
        """
        :param session: An OAuth2 session with Sentinel Hub service
        :type session: SentinelHubSession or None
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
    def _execute_download(self, request: DownloadRequest) -> Any:
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
                return response.content

            LOGGER.debug("Request needs to wait. Sleeping for %0.2f", sleep_time)
            time.sleep(sleep_time)

    def _execute_thread_safe(self, thread_unsafe_function: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Executes a function inside a thread lock and handles potential errors"""
        if self.lock is None:
            return thread_unsafe_function(*args, **kwargs)

        with self.lock:
            return thread_unsafe_function(*args, **kwargs)

    def _do_download(self, request: DownloadRequest) -> Any:
        """Runs the download"""
        return requests.request(
            request.request_type.value,
            url=request.url,
            json=request.post_values,
            headers=self._prepare_headers(request),
            timeout=self.config.download_timeout_seconds,
        )

    def _prepare_headers(self, request: DownloadRequest) -> Dict[str, Any]:
        """Prepares final headers by potentially joining them with session headers"""
        if not request.use_session:
            return request.headers

        session_headers = self._execute_thread_safe(self._get_session_headers)
        return {**session_headers, **request.headers}

    def _get_session_headers(self) -> Dict[str, Any]:
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

        cache_key = self.config.sh_client_id, self.config.sh_client_secret, self.config.get_sh_oauth_url()
        if cache_key in SentinelHubDownloadClient._CACHED_SESSIONS:
            session = SentinelHubDownloadClient._CACHED_SESSIONS[cache_key]
        else:
            session = SentinelHubSession(config=self.config)
            SentinelHubDownloadClient._CACHED_SESSIONS[cache_key] = session

        self.session = session
        return session

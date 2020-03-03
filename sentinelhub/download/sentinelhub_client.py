"""
Module implementing a rate-limited multi-threaded download client for downloading from Sentinel Hub service
"""
import logging
import time
from threading import Lock, currentThread

import requests

from .handlers import fail_user_errors, retry_temporal_errors
from .client import DownloadClient
from ..sentinelhub_session import SentinelHubSession
from ..sentinelhub_rate_limit import SentinelHubRateLimit


LOGGER = logging.getLogger(__name__)


class SentinelHubDownloadClient(DownloadClient):
    """ Download client specifically configured for download from Sentinel Hub service
    """
    _CACHED_SESSIONS = {}

    def __init__(self, *, session=None, **kwargs):
        """
        :param session: An OAuth2 session with Sentinel Hub service
        :type session: SentinelHubSession or None
        :param kwargs: Optional parameters from DownloadClient
        """
        super().__init__(**kwargs)

        if session is not None and not isinstance(session, SentinelHubSession):
            raise ValueError(f'A session parameter has to be an instance of {SentinelHubSession.__name__} or None, but '
                             f'{session} was given')
        self.session = session

        self.rate_limit = SentinelHubRateLimit(num_processes=self.config.number_of_download_processes)
        self.lock = Lock()

    @retry_temporal_errors
    @fail_user_errors
    def _execute_download(self, request):
        """ Executes the download with a single thread and uses a rate limit object, which is shared between all threads
        """
        thread_name = currentThread().getName()

        while True:
            sleep_time = self._execute_with_lock(self.rate_limit.register_next)

            if sleep_time == 0:
                response = self._do_download(request)

                self._execute_with_lock(self.rate_limit.update, response.headers)

                if response.status_code != requests.status_codes.codes.TOO_MANY_REQUESTS:
                    response.raise_for_status()

                    LOGGER.debug('%s: Successful download from %s', thread_name, request.url)
                    return response.content
            else:
                LOGGER.debug('%s: Sleeping for %0.2f', thread_name, sleep_time)
                time.sleep(sleep_time)

    def _execute_with_lock(self, thread_unsafe_function, *args, **kwargs):
        """ Executes a function inside a thread lock and handles potential errors
        """
        self.lock.acquire()
        try:
            return thread_unsafe_function(*args, **kwargs)
        finally:
            self.lock.release()

    def _do_download(self, request):
        """ Runs the download
        """
        return requests.request(
            request.request_type.value,
            url=request.url,
            json=request.post_values,
            headers=self._prepare_headers(request),
            timeout=self.config.download_timeout_seconds
        )

    def _prepare_headers(self, request):
        """ Prepares final headers by potentially joining them with session headers
        """
        if not request.use_session:
            return request.headers

        if self.session is None:
            self.session = self._execute_with_lock(self._get_session)

        return {
            **self.session.session_headers,
            **request.headers
        }

    def _get_session(self):
        """ Provides a session object either from cache or it creates a new one
        """
        cache_key = self.config.sh_client_id, self.config.sh_client_secret, self.config.get_sh_oauth_url()
        if cache_key in SentinelHubDownloadClient._CACHED_SESSIONS:
            return SentinelHubDownloadClient._CACHED_SESSIONS[cache_key]

        session = SentinelHubSession(config=self.config)
        SentinelHubDownloadClient._CACHED_SESSIONS[cache_key] = session
        return session

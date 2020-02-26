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
    def __init__(self, *, session=None, **kwargs):
        """
        :param session: An OAuth2 session with Sentinel Hub service
        :type session: SentinelHubSession or None
        :param kwargs: Optional parameters from DownloadClient
        """
        super().__init__(**kwargs)

        self.session = self._configure_session(session)

        self.rate_limit = SentinelHubRateLimit(num_processes=self.config.number_of_download_processes)
        self.lock = Lock()

    def _configure_session(self, session):
        """ Configures session object if credentials are given
        """
        if isinstance(session, SentinelHubSession):
            return session

        if session is None:
            if self.config.sh_client_id and self.config.sh_client_secret:
                return SentinelHubSession(config=self.config)

            return None

        raise ValueError('Given session should be an instance of SentinelHubSession')

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
            headers=self._prepare_headers(request.headers),
            timeout=self.config.download_timeout_seconds
        )

    def _prepare_headers(self, headers):
        """ Prepares final headers by potentially joining them with session headers
        """
        if self.session is None:
            return headers

        return {
            **self.session.session_headers,
            **headers
        }

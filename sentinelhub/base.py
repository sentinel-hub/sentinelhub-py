"""
Implementation of base interface classes of this package.
"""
import copy
import os
from abc import ABCMeta, abstractmethod
from typing import Any, Callable, Generic, Iterable, List, Optional, Tuple, TypeVar

from .config import SHConfig
from .download import DownloadClient, DownloadRequest
from .types import JsonDict

_T = TypeVar("_T")


class DataRequest(metaclass=ABCMeta):
    """A base abstract class for all implementations of data request interface.

    Every data request type can write the fetched data to disk and then read it again (and hence avoid the need to
    download the same data again).
    """

    def __init__(
        self, download_client_class: Callable, *, data_folder: Optional[str] = None, config: Optional[SHConfig] = None
    ):
        """
        :param download_client_class: A class implementing a download client
        :param data_folder: location of the directory where the fetched data will be saved.
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        self.download_client_class = download_client_class
        self.data_folder = data_folder
        self.config = config or SHConfig()

        self.download_list: List[DownloadRequest] = []
        self.folder_list: List[str] = []
        self.create_request()

    @abstractmethod
    def create_request(self) -> None:
        """An abstract method for logic of creating download requests"""

    def get_download_list(self) -> List[DownloadRequest]:
        """
        Returns a list of download requests for requested data.

        :return: List of data to be downloaded
        """
        return self.download_list

    def get_filename_list(self) -> List[str]:
        """Returns a list of file names (or paths relative to `data_folder`) where the requested data will be saved
        or read from, if it has already been downloaded and saved.

        :return: A list of filenames
        """
        return [request.get_relative_paths()[1] for request in self.download_list]

    def get_url_list(self) -> List[Optional[str]]:
        """
        Returns a list of urls for requested data.

        :return: List of URLs from where data will be downloaded.
        """
        return [request.url for request in self.download_list]

    def is_valid_request(self) -> bool:
        """Checks if initialized class instance successfully prepared a list of items to download

        :return: `True` if request is valid and `False` otherwise
        """
        return isinstance(self.download_list, list) and all(
            isinstance(request, DownloadRequest) for request in self.download_list
        )

    def get_data(
        self,
        *,
        save_data: bool = False,
        redownload: bool = False,
        data_filter: Optional[List[int]] = None,
        max_threads: Optional[int] = None,
        decode_data: bool = True,
        raise_download_errors: bool = True,
        show_progress: bool = False,
    ) -> List[Any]:
        """Get requested data either by downloading it or by reading it from the disk (if it
        was previously downloaded and saved).

        :param save_data: flag to turn on/off saving of data to disk. Default is `False`.
        :param redownload: if `True`, download again the requested data even though it's already saved to disk.
            Default is `False`, do not download if data is already available on disk.
        :param data_filter: Used to specify which items will be returned by the method and in which order. E.g. with
            ``data_filter=[0, 2, -1]`` the method will return only 1st, 3rd and last item. Default filter is `None`.
        :param max_threads: Maximum number of threads to be used for download in parallel. The default is
            `max_threads=None` which will use the number of processors on the system multiplied by 5.
        :param decode_data: If `True` it will return data in a decoded format, e.g. images in form of `numpy` arrays
            of values, JSON data in form of Python dictionaries, etc. Otherwise, it will return `DownloadResponse`
            objects which contain both encoded data in a binary format and metadata about response, e.g. response
            headers, status code, elapsed download time, etc.
        :param raise_download_errors: If `True` any error in download process should be raised as
            ``DownloadFailedException``. If `False` failed downloads will only raise warnings and the method will
            return list with `None` values in places where the results of failed download requests should be.
        :param show_progress: Whether a progress bar should be displayed while downloading.
        :return: requested images as numpy arrays, where each array corresponds to a single acquisition and has
            shape ``[height, width, channels]``.
        """
        self._preprocess_request(save_data, True)
        return self._execute_data_download(
            data_filter,
            redownload,
            max_threads,
            raise_download_errors,
            decode_data=decode_data,
            show_progress=show_progress,
        )

    def save_data(
        self,
        *,
        data_filter: Optional[List[int]] = None,
        redownload: bool = False,
        max_threads: Optional[int] = None,
        raise_download_errors: bool = False,
        show_progress: bool = False,
    ) -> None:
        """Saves data to disk. If ``redownload=True`` then the data is redownloaded using ``max_threads`` workers.

        :param data_filter: Used to specify which items will be returned by the method and in which order. E.g. with
            `data_filter=[0, 2, -1]` the method will return only 1st, 3rd and last item. Default filter is `None`.
        :param redownload: data is redownloaded if ``redownload=True``. Default is `False`
        :param max_threads: Maximum number of threads to be used for download in parallel. The default is
            `max_threads=None` which will use the number of processors on the system multiplied by 5.
        :param raise_download_errors: If `True` any error in download process should be raised as
            ``DownloadFailedException``. If `False` failed downloads will only raise warnings.
        :param show_progress: Whether a progress bar should be displayed while downloading.
        """
        self._preprocess_request(True, False)
        self._execute_data_download(
            data_filter, redownload, max_threads, raise_download_errors, show_progress=show_progress
        )

    def _execute_data_download(
        self,
        data_filter: Optional[List[int]] = None,
        redownload: bool = False,
        max_threads: Optional[int] = None,
        raise_download_errors: bool = False,
        decode_data: bool = True,
        show_progress: bool = False,
    ) -> List[Any]:
        """Calls download module and executes the download process

        :param data_filter: Used to specify which items will be returned by the method and in which order. E.g. with
            `data_filter=[0, 2, -1]` the method will return only 1st, 3rd and last item. Default filter is `None`.
        :param redownload: data is redownloaded if ``redownload=True``. Default is `False`
        :param max_threads: Maximum number of threads to be used for download in parallel. The default is
            `max_threads=None` which will use the number of processors on the system multiplied by 5.
        :param raise_download_errors: If `True` any error in download process should be raised as
            ``DownloadFailedException``. If `False` failed downloads will only raise warnings.
        :param decode_data: If `True` (default) it decodes data (e.g., returns image as an array of numbers);
            if `False` it returns binary data.
        :param show_progress: Whether a progress bar should be displayed while downloading.
        :return: List of data obtained from download
        """
        is_repeating_filter = False
        if data_filter is None:
            filtered_download_list = self.download_list
        elif isinstance(data_filter, (list, tuple)):
            try:
                filtered_download_list = [self.download_list[index] for index in data_filter]
            except IndexError as exception:
                raise IndexError("Indices of data_filter are out of range") from exception

            filtered_download_list, mapping_list = self._filter_repeating_items(filtered_download_list)
            is_repeating_filter = len(filtered_download_list) < len(mapping_list)
        else:
            raise ValueError("data_filter parameter must be a list of indices")

        client = self.download_client_class(
            redownload=redownload, raise_download_errors=raise_download_errors, config=self.config
        )
        data_list = client.download(
            filtered_download_list, max_threads=max_threads, decode_data=decode_data, show_progress=show_progress
        )

        if is_repeating_filter:
            data_list = [copy.deepcopy(data_list[index]) for index in mapping_list]

        return data_list

    @staticmethod
    def _filter_repeating_items(download_list: List[DownloadRequest]) -> Tuple[List[DownloadRequest], List[int]]:
        """Because of data_filter some requests in download list might be the same. In order not to download them again
        this method will reduce the list of requests. It will also return a mapping list which can be used to
        reconstruct the previous list of download requests.

        :param download_list: List of download requests
        :return: reduced download list with unique requests and mapping list
        """
        unique_requests_map = {}
        mapping_list = []
        unique_download_list: List[DownloadRequest] = []
        for download_request in download_list:
            if download_request not in unique_requests_map:
                unique_requests_map[download_request] = len(unique_download_list)
                unique_download_list.append(download_request)
            mapping_list.append(unique_requests_map[download_request])
        return unique_download_list, mapping_list

    def _preprocess_request(self, save_data: bool, return_data: bool) -> None:
        """Prepares requests for download and creates empty folders

        :param save_data: Tells whether to save data or not
        :param return_data: Tells whether to return data or not
        """
        if not self.is_valid_request():
            raise ValueError("Cannot obtain data because request is invalid")

        if save_data:
            if self.data_folder is None:
                raise ValueError(
                    "Request parameter `data_folder` is not specified. "
                    "In order to save data please set `data_folder` to location on your disk."
                )

            for folder in self.folder_list:
                os.makedirs(os.path.join(self.data_folder, folder), exist_ok=True)

        for download_request in self.download_list:
            download_request.save_response = save_data
            download_request.return_data = return_data
            download_request.data_folder = self.data_folder


class FeatureIterator(Generic[_T], metaclass=ABCMeta):
    """An implementation of a base feature iteration class

    Main functionalities:

    - The iterator will load only as many features as needed at any moment
    - It will keep downloaded features in memory so that iterating over it again will not have to download the same
      features again.
    """

    def __init__(self, client: DownloadClient, url: str, params: Optional[JsonDict] = None):
        """
        :param client: An instance of a download client object
        :param url: A URL where requests will be made
        :param params: Parameters to be sent with each request
        """
        self.client = client
        self.url = url
        self.params = params or {}

        self.index = 0
        self.features: List[_T] = []
        self.finished = False

    def __iter__(self) -> "FeatureIterator[_T]":
        """Method called at the beginning of a new iteration

        :return: It returns the iterator class itself
        """
        self.index = 0
        return self

    def __next__(self) -> _T:
        """Method called to provide the next feature in iteration

        :return: the next feature
        """
        while self.index >= len(self.features) and not self.finished:
            new_features = self._fetch_features()
            self.features.extend(new_features)

        if self.index < len(self.features):
            self.index += 1
            return self.features[self.index - 1]

        raise StopIteration

    @abstractmethod
    def _fetch_features(self) -> Iterable[_T]:
        """Collects and returns more features from the service"""

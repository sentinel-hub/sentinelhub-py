"""
Module implementing DownloadRequest class
"""
import hashlib
import json
import os
import warnings
import datetime as dt

from ..constants import MimeType, RequestType
from ..exceptions import SHRuntimeWarning
from ..os_utils import sys_is_windows


class DownloadRequest:
    """ A class defining a single download request.

    The class is a container with all parameters needed to execute a single download request and save or return
    downloaded data.
    """
    def __init__(self, *, url=None, headers=None, request_type=RequestType.GET, post_values=None, use_session=False,
                 data_type=MimeType.RAW, save_response=False, data_folder=None, filename=None, return_data=True,
                 **properties):
        """
        :param url: An URL from where to download
        :type url: str or None
        :param headers: Headers of a HTTP request
        :type headers: dict or None
        :param request_type: Type of request, either GET or POST. Default is `RequestType.GET`
        :type request_type: str or RequestType
        :param post_values: A dictionary of values that will be sent with a POST request. Default is `None`
        :type post_values: dict or None
        :param use_session: A flag that specifies if the download request will require a session header
        :type use_session: bool
        :param data_type: An expected file format of downloaded data. Default is `MimeType.RAW`
        :type data_type: MimeType or str
        :param save_response: A flag defining if the downloaded data will be saved to disk. Default is `False`.
        :type save_response: bool
        :param data_folder: A folder path where the fetched data will be (or already is) saved. Default is `None`
        :type data_folder: str or None
        :param filename: A custom filename where the data will be saved. By default data will be saved in a folder
            which name are hashed request parameters.
        :type filename: str or None
        :param return_data: A flag defining if the downloaded data will be returned as an output of download procedure.
            Default is `True`.
        :type return_data: bool
        :param properties: Any additional parameters.
        """
        self.url = url
        self.headers = headers or {}
        self.request_type = RequestType(request_type)
        self.post_values = post_values
        self.use_session = use_session

        self.data_type = MimeType(data_type)

        self.save_response = save_response
        self.data_folder = data_folder
        self.filename = filename
        self.return_data = return_data

        self.properties = properties

    def raise_if_invalid(self):
        """ Method that raises an error if something is wrong with request parameters

        :raises: ValueError
        """
        if self.save_response and self.data_folder is None:
            raise ValueError('Data folder is not specified. '
                             'Please give a data folder name in the initialization of your request.')

    def get_request_params(self, include_metadata=False):
        """ Provides parameters that define the request in form of a dictionary

        :param include_metadata: A flag defining if also metadata parameters should be included, such as headers and
            current time
        :type include_metadata: bool
        :return: A dictionary of parameters
        :rtype: dict
        """
        params = {
            'url': self.url,
            'payload': self.post_values
        }
        if include_metadata:
            params = {
                **params,
                'headers': self.headers,
                'timestamp': dt.datetime.now().isoformat()
            }
        return params

    def get_hashed_name(self):
        """ It takes request url and payload and calculates a unique hashed string from them.

        :return: A hashed string
        :rtype: str
        """
        params = self.get_request_params(include_metadata=False)
        hashable = json.dumps(params)

        return hashlib.md5(hashable.encode('utf-8')).hexdigest()

    def get_relative_paths(self):
        """ A method that calculates file paths relative to `data_folder`

        :return: Returns a pair of file paths, a request payload path and a response path. If request path is not
            defined it returns `None`.
        :rtype: (str or None, str)
        """

        if self.filename is not None:
            return None, self.filename

        hashed_name = self.get_hashed_name()

        request_path = os.path.join(hashed_name, 'request.json')
        response_path = os.path.join(hashed_name, f'response.{self.data_type.extension}')

        return request_path, response_path

    def get_storage_paths(self):
        """ A method that calculates file paths where request payload and response will be saved.

        :return: Returns a pair of file paths, a request payload path and a response path. Each of them can also be
            `None` if it is not defined.
        :rtype: (str or None, str or None)
        """
        if self.data_folder is None:
            return None, None

        request_path, response_path = self.get_relative_paths()

        if request_path is not None:
            request_path = os.path.join(self.data_folder, request_path)
        response_path = os.path.join(self.data_folder, response_path)

        self._check_path(response_path)
        return request_path, response_path

    @staticmethod
    def _check_path(file_path):
        """ Checks file path and warns about potential problems during saving
        """
        message_problem = None
        if len(file_path) > 255 and sys_is_windows():
            message_problem = 'File path'
        elif len(os.path.basename(file_path)) > 255:
            message_problem = 'Filename of'

        if message_problem:
            message = (
                f'{message_problem} {file_path} is longer than 255 character which might cause an error while '
                'saving on disk'
            )
            warnings.warn(message, category=SHRuntimeWarning)

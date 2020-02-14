"""
Module implementing DownloadRequest class
"""
import os
import warnings

from .cache import hash_request
from ..constants import MimeType, RequestType
from ..exceptions import SHRuntimeWarning
from ..os_utils import sys_is_windows


class DownloadRequest:
    """ Class to manage HTTP requests

    Container for all download requests issued by the DataRequests containing
    url to Sentinel Hub's services or other sources to data, file names to
    saved data and other necessary flags needed when the data is downloaded and
    interpreted.
    TODO
    :param url: url to Sentinel Hub's services or other sources from where the data is downloaded. Default is `None`
    :type url: str
    :param data_folder: folder name where the fetched data will be (or already is) saved. Default is `None`
    :type data_folder: str
    :param filename: filename of the file where the fetched data will be (or already is) saved. Default is `None`
    :type filename: str
    :param headers: add HTTP headers to request. Default is `None`
    :type headers: dict
    :param request_type: type of request, either GET or POST. Default is ``constants.RequestType.GET``
    :type request_type: constants.RequestType
    :param post_values: form encoded data to send in POST request. Default is `None`
    :type post_values: dict
    :param save_response: flag to turn on/off saving data downloaded by this request to disk. Default is `True`.
    :type save_response: bool
    :param return_data: flag to return or not data downloaded by this request to disk. Default is `True`.
    :type return_data: bool
    :param data_type: expected file format of downloaded data. Default is ``constants.MimeType.RAW``
    :type data_type: constants.MimeType
    """

    def __init__(self, *, url=None, headers=None, request_type=RequestType.GET, post_values=None,
                 data_type=MimeType.RAW, save_response=False, data_folder=None, filename=None, return_data=True,
                 **properties):

        self.url = url
        self.headers = headers or {}
        self.request_type = RequestType(request_type)
        self.post_values = post_values

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

    def get_saving_props(self):
        """ A method that calculates file paths of request payload and response

        :return: Returns a pair of file paths, representing path to request pa
        :rtype: (str or None, str or None, str or None)
        """
        if self.data_folder is None:
            return None, None, None

        if self.filename is None:
            hashed_name, request_info = hash_request(self.url, self.post_values)
            folder = os.path.join(self.data_folder, hashed_name)

            request_path = os.path.join(folder, 'request.json')
            response_path = os.path.join(folder, 'response.{}'.format(self.data_type.value))

            return request_path, request_info, response_path

        response_path = os.path.join(self.data_folder, self.filename)
        self._check_path(response_path)
        return None, None, response_path

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
            message = '{} {} is longer than 255 character which might cause an error while saving on ' \
                      'disk'.format(message_problem, file_path)
            warnings.warn(message, category=SHRuntimeWarning)

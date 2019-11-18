"""
Module implementing DownloadRequest class
"""

import os
import warnings

from ..constants import MimeType, RequestType
from ..exceptions import SHRuntimeWarning
from ..os_utils import sys_is_windows


class DownloadRequest:
    """ Class to manage HTTP requests

    Container for all download requests issued by the DataRequests containing
    url to Sentinel Hub's services or other sources to data, file names to
    saved data and other necessary flags needed when the data is downloaded and
    interpreted.

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

    def __init__(self, *, url=None, data_folder=None, filename=None, headers=None, request_type=RequestType.GET,
                 post_values=None, save_response=False, return_data=True, data_type=MimeType.RAW,
                 hash_save=False, **properties):

        if hash_save and data_folder is None:
            raise ValueError("When using hash_save, data_folder must also be set.")
        if hash_save and save_response:
            raise ValueError("Either hash_save or save_response should be set, not both.")

        self.url = url
        self.data_folder = data_folder
        self.filename = filename
        self.headers = {} if headers is None else headers
        self.post_values = post_values
        self.save_response = save_response
        self.return_data = return_data

        self.properties = properties

        self.request_type = RequestType(request_type)
        self.data_type = MimeType(data_type)
        self.hash_save = hash_save
        self.request_path = None

        self.file_path = self._file_path()

    def _file_path(self):
        if self.data_folder and self.filename:
            file_path = os.path.join(self.data_folder, self.filename.lstrip('/'))
        elif self.filename:
            file_path = self.filename
        else:
            file_path = None

        if file_path and len(file_path) > 255 and sys_is_windows():
            message = 'File path {} is longer than 255 character which might cause an error while saving on ' \
                      'disk'.format(self.file_path)
            warnings.warn(message, category=SHRuntimeWarning)

        elif file_path and len(self.filename) > 255:
            message = 'Filename {} is longer than 255 character which might cause an error while saving on ' \
                      'disk'.format(self.filename)
            warnings.warn(message, category=SHRuntimeWarning)

        return file_path

    def raise_if_invalid(self):
        """ Method that raises an error if something is wrong with request parameters

        :raises: ValueError
        """
        if self.save_response and self.data_folder is None:
            raise ValueError('Data folder is not specified. '
                             'Please give a data folder name in the initialization of your request.')

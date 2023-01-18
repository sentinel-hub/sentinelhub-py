"""
Module implementing model classes to store download-related parameters and data.
"""
from __future__ import annotations

import datetime as dt
import functools
import hashlib
import json
import os
import platform
import warnings
from dataclasses import dataclass, field, fields
from typing import Any, Dict, Optional, Tuple

from requests import Response

from ..constants import MimeType, RequestType
from ..decoding import decode_data
from ..exceptions import SHRuntimeWarning
from ..io_utils import read_data, write_data
from ..types import JsonDict


@dataclass
class DownloadRequest:
    """A class defining a single download request.

    The class is a container with all parameters needed to execute a single download request and save or return
    downloaded data.

    :param url: A URL from where to download
    :param headers: Headers of an HTTP request
    :param request_type: Type of request, either GET or POST. Default is `RequestType.GET`
    :param post_values: A dictionary of values that will be sent with a POST request. Default is `None`
    :param use_session: A flag that specifies if the download request will require a session header
    :param data_type: An expected file format of downloaded data. Default is `MimeType.RAW`
    :param save_response: A flag defining if the downloaded data will be saved to disk. Default is `False`.
    :param data_folder: A folder path where the fetched data will be (or already is) saved. Default is `None`
    :param filename: A custom filename where the data will be saved. By default, data will be saved in a folder
        which name are hashed request parameters.
    :param return_data: A flag defining if the downloaded data will be returned as an output of download procedure.
        Default is `True`.
    :param extra_params: Any additional parameters.
    """

    url: Optional[str] = None
    headers: JsonDict = field(default_factory=dict)
    request_type: RequestType = RequestType.GET
    post_values: Optional[JsonDict] = None
    use_session: bool = False
    data_type: MimeType = MimeType.RAW
    save_response: bool = False
    data_folder: Optional[str] = None
    filename: Optional[str] = None
    return_data: bool = True
    extra_params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Additional parsing of init parameters."""
        self.request_type = RequestType(self.request_type)
        self.data_type = MimeType(self.data_type)

    def __hash__(self) -> int:
        """This dataclass is mutable, but we still assign its id as its hash."""
        return id(self)

    def raise_if_invalid(self) -> None:
        """Method that raises an error if something is wrong with request parameters

        :raises: ValueError
        """
        if self.save_response and self.data_folder is None:
            raise ValueError(
                "Data folder is not specified. Please give a data folder name in the initialization of your request."
            )

    def get_request_params(self, include_metadata: bool = False) -> JsonDict:
        """Provides parameters that define the request in form of a dictionary

        :param include_metadata: A flag defining if also metadata parameters should be included, such as headers and
            current time
        :return: A dictionary of parameters
        """
        params = {"url": self.url, "payload": self.post_values}
        if include_metadata:
            params = {**params, "headers": self.headers, "timestamp": dt.datetime.now().isoformat()}
        return params

    def get_hashed_name(self) -> str:
        """It takes request url and payload and calculates a unique hashed string from them.

        :return: A hashed string
        """
        params = self.get_request_params(include_metadata=False)
        hashable = json.dumps(params)

        return hashlib.md5(hashable.encode("utf-8")).hexdigest()

    def get_relative_paths(self) -> Tuple[Optional[str], str]:
        """A method that calculates file paths relative to `data_folder`

        :return: Returns a pair of file paths, a request payload path and a response path. If request path is not
            defined it returns `None`.
        """

        if self.filename is not None:
            return None, self.filename

        hashed_name = self.get_hashed_name()

        request_path = os.path.join(hashed_name, "request.json")
        response_path = os.path.join(hashed_name, f"response.{self.data_type.extension}")

        return request_path, response_path

    def get_storage_paths(self) -> Tuple[Optional[str], Optional[str]]:
        """A method that calculates file paths where request payload and response will be saved.

        :return: Returns a pair of file paths, a request payload path and a response path. Each of them can also be
            `None` if it is not defined.
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
    def _check_path(file_path: str) -> None:
        """Checks file path and warns about potential problems during saving"""
        message_problem = None
        if len(file_path) > 255 and platform.system() == "Windows":
            message_problem = "File path"
        elif len(os.path.basename(file_path)) > 255:
            message_problem = "Filename of"

        if message_problem:
            message = (
                f"{message_problem} {file_path} is longer than 255 character which might cause an error while "
                "saving on disk"
            )
            warnings.warn(message, category=SHRuntimeWarning)


@dataclass(frozen=True)
class DownloadResponse:
    """A class defining a single download response.

    :param request: A download request object for which the response is obtained.
    :param content: Raw encoded data of the response.
    :param headers: Headers obtained with the response.
    :param status_code: Status code of the response.
    :param elapsed: Number of seconds it took to obtain the response.
    """

    request: DownloadRequest
    content: bytes
    headers: JsonDict = field(default_factory=dict)
    status_code: Optional[int] = None
    elapsed: Optional[float] = None

    @classmethod
    def from_response(cls, response: Response, request: DownloadRequest) -> DownloadResponse:
        """Creates `DownloadResponse` object from obtained from a service response and request info.

        :param: A service response object.
        :param: A request for which response was obtained.
        :return: An instance of a download response object.
        """
        return cls(
            request=request,
            content=response.content,
            headers=dict(response.headers),
            status_code=response.status_code,
            elapsed=response.elapsed.total_seconds(),
        )

    @classmethod
    def from_local(cls, request: DownloadRequest) -> DownloadResponse:
        """Creates `DownloadResponse` object by loading it from locally cached data.

        :param request: A request object for which data is cached locally.
        :return: An instance of a download response object.
        """
        request_path, response_path = request.get_storage_paths()
        if response_path is None:
            raise ValueError("Cannot load cached data because response path isn't defined")

        content = read_data(response_path, data_format=MimeType.RAW)

        response_builder = functools.partial(cls, request=request, content=content)
        if request_path is None:
            return response_builder()

        response_info = read_data(request_path, data_format=MimeType.JSON).get("response")
        if response_info is None:
            return response_builder()

        return response_builder(
            headers=response_info.get("headers", {}),
            status_code=response_info.get("status_code"),
            elapsed=response_info.get("elapsed"),
        )

    def to_local(self) -> None:
        """Caches data about a request and a response locally."""
        request_path, response_path = self.request.get_storage_paths()
        if response_path is None:
            raise ValueError("Cannot cache data because response path isn't defined")

        write_data(response_path, self.content, data_format=MimeType.RAW)

        if request_path is None:
            return

        info = {
            "request": self.request.get_request_params(include_metadata=True),
            "response": {
                "headers": self.headers,
                "status_code": self.status_code,
                "elapsed": self.elapsed,
            },
        }
        write_data(request_path, info, data_format=MimeType.JSON)

    @property
    def response_type(self) -> MimeType:
        """Provides the expected mime type of the response data."""
        if self.request.data_type is not MimeType.RAW:
            return self.request.data_type

        content_type = self.headers.get("Content-Type") or self.headers.get("content-type")
        if content_type:
            return MimeType.from_string(content_type)
        return MimeType.RAW

    def decode(self) -> Any:
        """Decodes binary data into a Python object."""
        return decode_data(self.content, data_type=self.response_type)

    def derive(self, **params: Any) -> DownloadResponse:
        """Create a new response by changing some parameters of the existing one.

        :param params: Any of `DownloadResponse` attributes.
        :return: A new instance of `DownloadResponse` with modified parameters
        """
        derived_params = {_field.name: getattr(self, _field.name) for _field in fields(self)}
        derived_params.update(params)

        return DownloadResponse(**derived_params)

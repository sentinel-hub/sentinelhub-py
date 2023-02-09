"""
The core module for Geopedia interactions
"""
from __future__ import annotations

import datetime
import hashlib
from typing import TYPE_CHECKING, Any, Iterable, Iterator, List, Optional, Union, overload

from shapely.geometry import shape as geo_shape
from shapely.geometry.base import BaseGeometry
from typing_extensions import Literal

from ..api.ogc import OgcImageService, OgcRequest
from ..base import FeatureIterator
from ..config import SHConfig
from ..constants import CRS, MimeType
from ..download import DownloadClient, DownloadRequest
from ..geometry import BBox
from ..types import JsonDict

if TYPE_CHECKING:
    from .request import GeopediaImageRequest


class GeopediaService:
    """The class for Geopedia OGC services"""

    def __init__(self, config: Optional[SHConfig] = None):
        """
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        self.config = config or SHConfig()
        self._base_url = self.config.geopedia_rest_url


@overload
def _parse_geopedia_layer(layer: Union[int, str], return_wms_name: Literal[False] = False) -> int:
    ...


@overload
def _parse_geopedia_layer(layer: Union[int, str], return_wms_name: Literal[True]) -> str:
    ...


def _parse_geopedia_layer(layer: Union[int, str], return_wms_name: bool = False) -> Union[int, str]:
    """Helper function for parsing Geopedia layer name. If WMS name is required and wrong form is given it will
    return a string with 'ttl' at the beginning. (WMS name can also start with something else, e.g. only 't'
    instead 'ttl', therefore anything else is also allowed.) Otherwise, it will parse it into a number.
    """
    if not isinstance(layer, (int, str)):
        raise ValueError(f"Parameter 'layer' should be an integer or a string, but {type(layer)} found")

    if return_wms_name:
        if isinstance(layer, int) or layer.isdigit():
            return f"ttl{layer}"
        return layer

    if isinstance(layer, str):
        stripped_layer = layer.lstrip("tl")
        if not stripped_layer.isdigit():
            raise ValueError(f"Parameter 'layer' has unsupported value {layer}, expected an integer")
        layer = stripped_layer

    return int(layer)


class GeopediaSession(GeopediaService):
    """For retrieving data from Geopedia vector and raster layers it is required to make a session. This class handles
    starting and renewing of session and login (optional). It provides session headers required by Geopedia REST
    requests. Session duration is hardcoded to 1 hour with class attribute SESSION_DURATION. After that this
    class will automatically renew the session and login.
    """

    SESSION_DURATION = datetime.timedelta(hours=1)
    UNAUTHENTICATED_USER_ID = "NO_USER"

    _global_session_info = None
    _global_session_start = None

    def __init__(
        self,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        password_md5: Optional[str] = None,
        is_global: bool = False,
        **kwargs: Any,
    ):
        """
        :param username: Optional parameter in case of login with Geopedia credentials
        :param password: Optional parameter in case of login with Geopedia credentials
        :param password_md5: Password can optionally also be provided as already encoded md5 hexadecimal string
        :param is_global: If `True` this session will be shared among all instances of this class, otherwise it will be
            used only with the single instance. Default is `False`.
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        super().__init__(**kwargs)

        if password and password_md5:
            raise ValueError("At most one of the parameters 'password' and 'password_md5' can be specified, not both")

        self.username = username
        self.password = password_md5 if password is None else hashlib.md5(password.encode()).hexdigest()
        self.is_global = is_global

        if bool(self.username) != bool(self.password):
            raise ValueError(
                "Either both username and password have to be specified or neither of them, only one found"
            )

        self._session_info: Optional[dict] = None
        self._session_start: Optional[datetime.datetime] = None

        self.provide_session()

    @property
    def session_info(self) -> dict:
        """All information that Geopedia provides about the current session

        :return: A dictionary with session info
        """
        return self.provide_session()

    @property
    def session_id(self) -> str:
        """A public property of this class which provides a Geopedia session ID

        :return: A session ID string
        """
        return self._parse_session_id(self.provide_session())

    @property
    def session_headers(self) -> dict:
        """Headers which have to be used when accessing any data from Geopedia with this session

        :return: A dictionary containing session headers
        """
        session_info = self.provide_session()
        return {session_info["sessionHeaderName"]: self._parse_session_id(session_info)}

    @property
    def user_info(self) -> dict:
        """Information that this session has about user

        :return: A dictionary with user info
        """
        return self.provide_session()["user"]

    @property
    def user_id(self) -> str:
        """Geopedia user ID. If no login was done during session this will return `'NO_USER'`.

        :return: User ID string
        """
        return self._parse_user_id(self.provide_session())

    def restart(self) -> "GeopediaSession":
        """Method that restarts Geopedia Session

        :return: It returns the object itself, with new session
        """
        self.provide_session(start_new=True)
        return self

    def provide_session(self, start_new: bool = False) -> dict:
        """Makes sure that session is still valid and provides session info

        :param start_new: If `True` it will always create a new session. Otherwise, it will create a new
            session only if no session exists or the previous session timed out.
        :return: Current session info
        """
        if self.is_global:
            self._session_info = self._global_session_info
            self._session_start = self._global_session_start

        if (
            self._session_info is None
            or self._session_start is None
            or start_new
            or datetime.datetime.now() > self._session_start + self.SESSION_DURATION
        ):
            self._start_new_session()

        return self._session_info  # type: ignore[return-value]

    def _start_new_session(self) -> None:
        """Starts a new session and calculates when the new session will end. If username and password are provided
        it will also make login.
        """
        self._session_start = datetime.datetime.now()

        session_id = self._parse_session_id(self._session_info) if self._session_info else ""
        session_url = f"{self._base_url}/data/v1/session/create?locale=en&sid={session_id}"

        client = DownloadClient(config=self.config)
        self._session_info = client.get_json_dict(session_url)

        if self.username and self.password and self._parse_user_id(self._session_info) == self.UNAUTHENTICATED_USER_ID:
            self._make_login(self._session_info)

        if self.is_global:
            GeopediaSession._global_session_info = self._session_info
            GeopediaSession._global_session_start = self._session_start

    def _make_login(self, session_info: dict) -> None:
        """Private method that makes login"""
        login_url = (
            f"{self._base_url}/data/v1/session/login?user={self.username}&pass={self.password}"
            f"&sid={self._parse_session_id(session_info)}"
        )
        client = DownloadClient(config=self.config)
        self._session_info = client.get_json_dict(login_url)

    @staticmethod
    def _parse_session_id(session_info: dict) -> str:
        """Method for parsing session ID from session info"""
        return session_info["sessionId"]

    @staticmethod
    def _parse_user_id(session_info: dict) -> str:
        """Method for parsing user ID from session info"""
        return session_info["user"]["id"]


class GeopediaWmsService(GeopediaService, OgcImageService):
    """Geopedia OGC services class for providing image data. Most of the methods are inherited from
    `sentinelhub.ogc.OgcImageService` class.
    """

    def __init__(self, **kwargs: Any):
        """
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        super().__init__(**kwargs)

        self._base_url = self.config.geopedia_wms_url

    def get_request(self, request: OgcRequest) -> List[DownloadRequest]:
        """Get a list of DownloadRequests for all data that are under the given field in the table of a Geopedia layer.

        :return: list of items which have to be downloaded
        """
        request.layer = _parse_geopedia_layer(request.layer, return_wms_name=True)

        return super().get_request(request)

    def get_dates(self, _: OgcRequest) -> List[Optional[datetime.datetime]]:
        """Geopedia does not support date queries

        :param request: OGC-type request
        :return: Undefined date
        """
        return [None]

    def get_wfs_iterator(self) -> Any:
        """This method is inherited from OgcImageService but is not implemented."""
        raise NotImplementedError


class GeopediaImageService(GeopediaService):
    """Service class that provides images from a Geopedia vector layer."""

    def __init__(self, **kwargs: Any):
        """
        :param base_url: Base url of Geopedia REST services. If `None`, the url
                     specified in the configuration file is taken.
        """
        super().__init__(**kwargs)

        self.gpd_iterator: Optional[GeopediaFeatureIterator] = None

    def get_request(self, request: GeopediaImageRequest) -> List[DownloadRequest]:
        """Get a list of DownloadRequests for all data that are under the given field in the table of a Geopedia layer.

        :return: list of items which have to be downloaded
        """
        return [
            DownloadRequest(
                url=self._get_url(item), filename=self._get_filename(request, item), data_type=request.image_format
            )
            for item in self._get_items(request)
        ]

    def _get_items(self, request: GeopediaImageRequest) -> list:
        """Collects data from Geopedia layer and returns list of features"""
        if request.gpd_iterator is None:
            self.gpd_iterator = GeopediaFeatureIterator(
                request.layer, bbox=request.bbox, config=self.config, gpd_session=request.gpd_session
            )
        else:
            self.gpd_iterator = request.gpd_iterator

        field_iter = self.gpd_iterator.get_field_iterator(request.image_field_name)
        items = []

        for field_items in field_iter:  # an image field can have multiple images
            for item in field_items:
                if not item["mimeType"].startswith("image/"):
                    continue

                mime_type = MimeType.from_string(item["mimeType"][6:])

                if mime_type is request.image_format:
                    items.append(item)

        return items

    @staticmethod
    def _get_url(item: dict) -> Optional[str]:
        return item.get("objectPath")

    @staticmethod
    def _get_filename(request: GeopediaImageRequest, item: dict) -> Optional[str]:
        """Creates a filename"""
        if request.keep_image_names:
            return item["niceName"]
        return None

    def get_gpd_iterator(self) -> Optional[GeopediaFeatureIterator]:
        """Returns iterator over info about data used for the `GeopediaVectorRequest`

        :return: Iterator of dictionaries containing info about data used in the request.
        """
        return self.gpd_iterator


class GeopediaFeatureIterator(FeatureIterator[JsonDict]):
    """Iterator for Geopedia Vector Service"""

    FILTER_EXPRESSION = "filterExpression"
    MAX_FEATURES_PER_REQUEST = 1000

    def __init__(
        self,
        layer: Union[str, int],
        bbox: Optional[BBox] = None,
        query_filter: Optional[str] = None,
        offset: int = 0,
        gpd_session: Optional[GeopediaSession] = None,
        config: Optional[SHConfig] = None,
    ):
        """
        :param layer: Geopedia layer which contains requested data
        :param bbox: Bounding box of the requested image. Its coordinates must be in the CRS.POP_WEB (EPSG:3857)
            coordinate system.
        :param query_filter: Query string used for filtering returned features.
        :param offset: Offset of resulting features
        :param gpd_session: Optional parameter for specifying a custom Geopedia session, which can also contain login
            credentials. This can be used for accessing private Geopedia layers. By default, it is set to `None` and a
            basic Geopedia session without credentials will be created.
        :param config: A custom instance of config class to override parameters from the saved configuration.
        """
        self.layer = _parse_geopedia_layer(layer)
        self.config = config or SHConfig()
        self.gpd_session = gpd_session if gpd_session else GeopediaSession(is_global=True)

        client = DownloadClient(config=self.config)
        url = f"{self.config.geopedia_rest_url}/data/v2/search/tables/{self.layer}/features"
        params = self._build_request_params(bbox, query_filter)

        super().__init__(client, url, params)
        self.next = f"{url}?offset={offset}&limit={self.MAX_FEATURES_PER_REQUEST}"

        self.layer_size: Optional[int] = None

    def _build_request_params(self, bbox: Optional[BBox], query_filter: Optional[str]) -> dict:
        """Builds payload parameters for requests to Geopedia"""
        params = {}
        if bbox is not None:
            if bbox.crs is not CRS.POP_WEB:
                bbox = bbox.transform(CRS.POP_WEB)

            params[self.FILTER_EXPRESSION] = f'bbox({",".join(map(str, bbox))},"EPSG:3857")'

        if query_filter is not None:
            if self.FILTER_EXPRESSION in params:
                params[self.FILTER_EXPRESSION] = f"{params[self.FILTER_EXPRESSION]} && ({query_filter})"
            else:
                params[self.FILTER_EXPRESSION] = query_filter

        return params

    def __len__(self) -> int:
        """Length of iterator is number of features which can be obtained from Geopedia with applied filters"""
        return self.get_size()

    def _fetch_features(self) -> Iterable[JsonDict]:
        """Retrieves a new page of features from Geopedia"""
        response = self.client.get_json_dict(
            self.next, post_values=self.params, headers=self.gpd_session.session_headers
        )

        new_features = response["features"]
        pagination = response["pagination"]

        self.layer_size = pagination.get("total")
        self.next = pagination.get("next")

        if not self.next or not new_features:
            self.finished = True

        elif "offset=" not in self.next or f"limit={self.MAX_FEATURES_PER_REQUEST}" not in self.next:
            raise ValueError(f"Next page does not have an offset or correct limit parameter: {self.next}")

        return new_features

    def get_geometry_iterator(self) -> Iterator[BaseGeometry]:
        """Iterator over Geopedia feature geometries"""
        for feature in self:
            yield geo_shape(feature["geometry"])

    def get_field_iterator(self, field: str) -> Iterator[Any]:
        """Iterator over the specified field of Geopedia features"""
        for feature in self:
            yield feature["properties"].get(field, [])

    def get_size(self) -> int:
        """Provides number of features which can be obtained. It has to fetch at least one feature from
        Geopedia services to get this information.

        :return: Size of Geopedia layer with applied filters
        """
        if self.layer_size is None:
            new_features = self._fetch_features()
            self.features.extend(new_features)

        return self.layer_size  # type: ignore[return-value]

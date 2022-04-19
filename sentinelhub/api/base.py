"""
Module implementing some utility functions not suitable for other utility modules
"""
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Union
from urllib.parse import urlencode

from dataclasses_json import CatchAll, LetterCase, Undefined
from dataclasses_json import config as dataclass_config
from dataclasses_json import dataclass_json

from ..base import FeatureIterator
from ..config import SHConfig
from ..data_collections import DataCollection
from ..download.sentinelhub_client import SentinelHubDownloadClient
from ..exceptions import MissingDataInRequestException, SHDeprecationWarning
from .utils import datetime_config, remove_undefined


class SentinelHubService:
    """A base class for classes interacting with different Sentinel Hub APIs"""

    def __init__(self, config=None, base_url=None):
        """
        :param config: A configuration object with required parameters `sh_client_id`, `sh_client_secret`, and
            `sh_auth_base_url` which is used for authentication and `sh_base_url` which defines the service
            deployment that will be used.
        :type config: SHConfig or None
        :param base_url: A deprecated parameter. Use `config` instead.
        :type base_url: str or None
        """
        self.config = config or SHConfig()

        if base_url:
            warnings.warn(
                "Parameter base_url is deprecated and will soon be removed. Instead set "
                "config.sh_base_url = base_url and provide it with config parameter",
                category=SHDeprecationWarning,
            )

        base_url = base_url or self.config.sh_base_url
        if not isinstance(base_url, str):
            raise ValueError(f"Sentinel Hub base URL parameter should be a string but got {base_url}")
        base_url = base_url.rstrip("/")
        self.service_url = self._get_service_url(base_url)

        self.client = SentinelHubDownloadClient(config=self.config)

    @staticmethod
    def _get_service_url(base_url):
        """Provides the URL to a specific service"""
        raise NotImplementedError


class SentinelHubFeatureIterator(FeatureIterator):
    """Feature iterator for the most common implementation of feature pagination at Sentinel Hub services"""

    def __init__(self, *args, exception_message=None, **kwargs):
        """
        :param args: Arguments passed to FeatureIterator
        :param exception_message: A message to be raised if no features are found
        :type exception_message: str
        :param kwargs: Keyword arguments passed to FeatureIterator
        """
        self.exception_message = exception_message or "No data found"

        super().__init__(*args, **kwargs)

    def _fetch_features(self):
        """Collect more results from the service"""
        params = remove_undefined({**self.params, "viewtoken": self.next})
        url = f"{self.url}?{urlencode(params)}"

        results = self.client.get_json(url, use_session=True)

        new_features = results.get("data")
        if new_features is None:
            raise MissingDataInRequestException(self.exception_message)

        self.next = results["links"].get("nextToken")
        self.finished = self.next is None or not new_features

        return new_features


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass
class BaseCollection:
    """Dataclass to hold data about a collection"""

    name: str
    s3_bucket: str
    other_data: CatchAll
    collection_id: Optional[str] = field(metadata=dataclass_config(field_name="id"), default=None)
    user_id: Optional[str] = None
    created: Optional[datetime] = field(metadata=datetime_config, default=None)
    no_data: Optional[Union[int, float]] = None

    def to_data_collection(self):
        """Returns a DataCollection enum for this collection"""
        if self.collection_id is None:
            raise ValueError("This collection is missing a collection id")

        if self.additional_data and self.additional_data.bands:
            band_names = tuple(self.additional_data.bands)
        else:
            band_names = None

        return DataCollection.define_byoc(collection_id=self.collection_id, bands=band_names)

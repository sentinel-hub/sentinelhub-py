"""
Module implementing some utility functions not suitable for other utility modules
"""
from abc import ABC, abstractmethod
from dataclasses import field, dataclass
from datetime import datetime
from typing import Optional, Union
from urllib.parse import urlencode

from dataclasses_json import config as dataclass_config
from dataclasses_json import dataclass_json, LetterCase, Undefined, CatchAll

from .data_collections import DataCollection
from .geometry import Geometry
from .exceptions import MissingDataInRequestException
from .time_utils import parse_time, serialize_time


datetime_config = dataclass_config(
    encoder=lambda time: serialize_time(time, use_tz=True) if time else None,
    decoder=lambda time: parse_time(time, force_datetime=True) if time else None,
    letter_case=LetterCase.CAMEL
)


geometry_config = dataclass_config(encoder=Geometry.get_geojson,
                                   decoder=lambda x: Geometry.from_geojson(x) if x else None,
                                   exclude=lambda x: x is None, letter_case=LetterCase.CAMEL)


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass
class BaseCollection:
    """ Dataclass to hold data about a collection
    """
    name: str
    s3_bucket: str
    other_data: CatchAll
    collection_id: Optional[str] = field(metadata=dataclass_config(field_name='id'), default=None)
    user_id: Optional[str] = None
    created: Optional[datetime] = field(metadata=datetime_config, default=None)
    no_data: Optional[Union[int, float]] = None

    def to_data_collection(self):
        """ Returns a DataCollection enum for this collection
        """
        if self.collection_id is None:
            raise ValueError('This collection is missing a collection id')

        if self.additional_data and self.additional_data.bands:
            band_names = tuple(self.additional_data.bands)
        else:
            band_names = None

        return DataCollection.define_byoc(collection_id=self.collection_id, bands=band_names)


class FeatureIterator(ABC):
    """ An implementation of a base feature iteration class

    Main functionalities:

    - The iterator will load only as many features as needed at any moment
    - It will keep downloaded features in memory so that iterating over it again will not have to download the same
      features again.
    """
    def __init__(self, client, url, params=None):
        """
        :param client: An instance of a download client object
        :type client: DownloadClient
        :param url: An URL where requests will be made
        :type url: str
        :param params: Parameters to be sent with each request
        :type params: dict or None
        """
        self.client = client
        self.url = url
        self.params = params or {}

        self.index = 0
        self.features = []
        self.next = None
        self.finished = False

    def __iter__(self):
        """ Method called at the beginning of a new iteration

        :return: It returns the iterator class itself
        :rtype: FeatureIterator
        """
        self.index = 0
        return self

    def __next__(self):
        """ Method called to provide the next feature in iteration

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
    def _fetch_features(self):
        """ Collects and returns more features from the service
        """
        raise NotImplementedError


class SentinelHubFeatureIterator(FeatureIterator):
    """ Feature iterator for the most common implementation of feature pagination at Sentinel Hub services
    """
    def __init__(self, *args, exception_message=None, **kwargs):
        """
        :param args: Arguments passed to FeatureIterator
        :param exception_message: A message to be raise if no feature are found
        :type exception_message: str
        :param kwargs: Keyword arguments passed to FeatureIterator
        """
        self.exception_message = exception_message or 'No data found'

        super().__init__(*args, **kwargs)

    def _fetch_features(self):
        """ Collect more results from the service
        """
        params = remove_undefined({
            **self.params,
            'viewtoken': self.next
        })
        url = f'{self.url}?{urlencode(params)}'

        results = self.client.get_json(url, use_session=True)

        new_features = results.get('data')
        if new_features is None:
            raise MissingDataInRequestException(self.exception_message)

        self.next = results['links'].get('nextToken')
        self.finished = self.next is None or not new_features

        return new_features


def _update_other_args(dict1, dict2):
    """ Function for a recursive update of `dict1` with `dict2`. The function loops over the keys in `dict2` and
    only the non-dict like values are assigned to the specified keys.
    """
    for key, value in dict2.items():
        if isinstance(value, dict) and key in dict1:
            _update_other_args(dict1[key], value)
        else:
            dict1[key] = value


def remove_undefined(payload):
    """ Takes a dictionary and removes keys without value
    """
    return {name: value for name, value in payload.items() if value is not None}

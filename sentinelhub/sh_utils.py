"""
Module implementing some utility functions not suitable for other utility modules
"""
from abc import ABC, abstractmethod
from urllib.parse import urlencode

from sentinelhub.exceptions import MissingDataInRequestException


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


def remove_undefined(payload):
    """ Takes a dictionary and removes keys without value
    """
    return {name: value for name, value in payload.items() if value is not None}

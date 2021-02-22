"""
Module implementing some utility functions not suitable for other utility modules
"""
from abc import ABC, abstractmethod
from urllib.parse import urlencode

from sentinelhub.config import SHConfig
from sentinelhub.download import SentinelHubDownloadClient
from sentinelhub.exceptions import MissingDataInRequestException


class FeatureIterator(ABC):
    """ An implementation of a base feature iteration class

    Main functionalities:
    - The iterator will load only as many features as needed at any moment
    - It will keep downloaded features in memory so that iterating over it again will not have to download the same
    features again.
    """
    def __init__(self, client, url, params):
        """
        :param client: An instance of a download client object
        :type client: DownloadClient
        :param url: An URL where requests will be made
        :type url: str
        :param params: Parameters to be sent with each request
        :type params: dict
        """
        self.client = client
        self.url = url
        self.params = params

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


def iter_pages(service_url, client=None, config=None, exception_message=None, **params):
    """ Iterates over pages of items retrieved from Sentinel-Hub service
    """

    config = config or SHConfig()
    client = client or SentinelHubDownloadClient(config=config)

    token = None
    exception_message = exception_message or 'No data.'

    while True:
        if token is not None:
            params['viewtoken'] = token

        url = f'{service_url}?{urlencode(params)}'
        results = client.get_json(url, use_session=True)

        results_data = results.get('data')
        if results_data is None:
            raise MissingDataInRequestException(exception_message)

        for item in results_data:
            yield item

        token = results['links'].get('nextToken')
        if token is None:
            break


def remove_undefined(payload):
    """ Takes a dictionary and removes keys without value
    """
    return {name: value for name, value in payload.items() if value is not None}

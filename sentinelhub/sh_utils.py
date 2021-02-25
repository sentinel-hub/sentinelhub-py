"""
Module implementing some utility functions not suitable for other utility modules
"""
from urllib.parse import urlencode

from sentinelhub.config import SHConfig
from sentinelhub.download import SentinelHubDownloadClient
from sentinelhub.exceptions import MissingDataInRequestException


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

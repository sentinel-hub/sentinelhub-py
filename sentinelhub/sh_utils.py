"""
Module implementing some utility functions not suitable for other utility modules
"""
from datetime import datetime
from urllib.parse import urlencode

import dateutil

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


def from_sh_datetime(date_time):
    """ Parse datetime from SH service (which is always in UTC, but missing 'Z')
    """
    if isinstance(date_time, datetime):
        return date_time
    if isinstance(date_time, str):
        return dateutil.parser.parse(date_time.replace('Z', '+00:00'))
    return None


def to_sh_datetime(date_time):
    """ Parse datetime to what SH service expects
    """
    if date_time:
        if datetime.tzinfo:
            return datetime.isoformat(date_time)
        return datetime.isoformat(date_time) + 'Z'
    return None

"""
Module for communication with http://opensearch.sentinel-hub.com/resto/api

For more search parameters check: http://opensearch.sentinel-hub.com/resto/api/collections/Sentinel2/describe.xml
"""
import logging
from urllib.parse import urlencode

from .constants import CRS
from .config import SHConfig
from .download import DownloadClient
from .time_utils import parse_time_interval, parse_time, serialize_time


LOGGER = logging.getLogger(__name__)


class TileMissingException(Exception):
    """ This exception is raised when requested tile is missing at Sentinel Hub Opensearch service
    """


def get_tile_info_id(tile_id):
    """ Get basic information about image tile

    :param tile_id: original tile identification string provided by ESA (e.g.
                    'S2A_OPER_MSI_L1C_TL_SGS__20160109T230542_A002870_T10UEV_N02.01')
    :type tile_id: str
    :return: dictionary with info provided by Opensearch REST service or `None` if such tile does not exist on AWS.
    :rtype: dict or None
    :raises: TileMissingException if no tile with tile ID `tile_id` exists
    """
    result_list = list(search_iter(tile_id=tile_id))

    if not result_list:
        raise TileMissingException

    if len(result_list) > 1:
        LOGGER.warning('Obtained %d results for tile_id=%s. Returning the first one', len(result_list), tile_id)

    return result_list[0]


def get_tile_info(tile, time, aws_index=None, all_tiles=False):
    """ Get basic information about image tile

    :param tile: tile name (e.g. ``'T10UEV'``)
    :type tile: str
    :param time: A single date or a time interval
    :type time: str or (str, str) or datetime.date or (datetime.date, datetime.date)
    :param aws_index: index of tile on AWS
    :type aws_index: int or None
    :param all_tiles: If `True` it will return list of all tiles otherwise only the first one
    :type all_tiles: bool
    :return: dictionary with info provided by Opensearch REST service or `None` if such tile does not exist on AWS.
    :rtype: dict or None
    """
    start_date, end_date = parse_time_interval(time)

    candidates = []
    for tile_info in search_iter(start_date=start_date, end_date=end_date):
        path_props = tile_info['properties']['s3Path'].split('/')
        this_tile = ''.join(path_props[1: 4])
        this_aws_index = int(path_props[-1])
        if this_tile == tile.lstrip('T0') and (aws_index is None or aws_index == this_aws_index):
            candidates.append(tile_info)

        if candidates and aws_index is not None:
            break
        if len(candidates) >= 2 and not all_tiles:
            break

    if not candidates:
        raise TileMissingException
    if all_tiles:
        return candidates

    if len(candidates) > 1:
        LOGGER.info('Obtained more than one result for tile=%s, time=%s. Returning the first one', tile, time)
    return candidates[0]


def get_area_info(bbox, date_interval, maxcc=None):
    """ Get information about all images from specified area and time range

    :param bbox: bounding box of requested area
    :type bbox: geometry.BBox
    :param date_interval: a pair of time strings in ISO8601 format
    :type date_interval: tuple(str)
    :param maxcc: filter images by maximum percentage of cloud coverage
    :type maxcc: float in range [0, 1] or None
    :return: list of dictionaries containing info provided by Opensearch REST service
    :rtype: list(dict)
    """
    result_list = search_iter(bbox=bbox, start_date=date_interval[0], end_date=date_interval[1])
    if maxcc:
        return reduce_by_maxcc(result_list, maxcc)
    return result_list


def get_area_dates(bbox, date_interval, maxcc=None):
    """ Get list of times of existing images from specified area and time range

    :param bbox: bounding box of requested area
    :type bbox: geometry.BBox
    :param date_interval: a pair of time strings in ISO8601 format
    :type date_interval: tuple(str)
    :param maxcc: filter images by maximum percentage of cloud coverage
    :type maxcc: float in range [0, 1] or None
    :return: list of time strings in ISO8601 format
    :rtype: list[datetime.datetime]
    """

    area_info = get_area_info(bbox, date_interval, maxcc=maxcc)
    return sorted({parse_time(tile_info['properties']['startDate']) for tile_info in area_info})


def reduce_by_maxcc(result_list, maxcc):
    """ Filter list image tiles by maximum cloud coverage

    :param result_list: list of dictionaries containing info provided by Opensearch REST service
    :type result_list: list(dict)
    :param maxcc: filter images by maximum percentage of cloud coverage
    :type maxcc: float in range [0, 1]
    :return: list of dictionaries containing info provided by Opensearch REST service
    :rtype: list(dict)
    """
    return [tile_info for tile_info in result_list if tile_info['properties']['cloudCover'] <= 100 * float(maxcc)]


def search_iter(tile_id=None, bbox=None, start_date=None, end_date=None, absolute_orbit=None, config=None):
    """ A generator function that implements OpenSearch search queries and returns results

    All parameters for search are optional.

    :param tile_id: original tile identification string provided by ESA (e.g.
                    'S2A_OPER_MSI_L1C_TL_SGS__20160109T230542_A002870_T10UEV_N02.01')
    :type tile_id: str
    :param bbox: bounding box of requested area
    :type bbox: geometry.BBox
    :param start_date: beginning of time range
    :type start_date: str
    :param end_date: end of time range
    :type end_date: str
    :param absolute_orbit: An absolute orbit number of Sentinel-2 L1C products as defined by ESA
    :type absolute_orbit: int
    :return: An iterator returning dictionaries with info provided by Sentinel Hub OpenSearch REST service
    :rtype: Iterator[dict]
    :param config: A custom instance of config class to override parameters from the saved configuration.
    :type config: SHConfig or None
    """
    config = config or SHConfig()

    if bbox and bbox.crs is not CRS.WGS84:
        bbox = bbox.transform(CRS.WGS84)

    start_date = parse_time(start_date) if start_date else None
    end_date = parse_time(end_date) if end_date else None

    url_params = _prepare_url_params(tile_id, bbox, end_date, start_date, absolute_orbit)
    url_params['maxRecords'] = config.max_opensearch_records_per_query

    start_index = 1
    client = DownloadClient(config=config)

    while True:
        url_params['index'] = start_index

        url = f'{config.opensearch_url}/search.json?{urlencode(url_params)}'
        LOGGER.debug("URL=%s", url)

        response = client.get_json(url)
        for tile_info in response["features"]:
            yield tile_info

        if len(response["features"]) < config.max_opensearch_records_per_query:
            break
        start_index += config.max_opensearch_records_per_query


def _prepare_url_params(tile_id, bbox, end_date, start_date, absolute_orbit):
    """ Constructs dict with URL params

    :param tile_id: original tile identification string provided by ESA (e.g.
                    'S2A_OPER_MSI_L1C_TL_SGS__20160109T230542_A002870_T10UEV_N02.01')
    :type tile_id: str
    :param bbox: bounding box of requested area in WGS84 CRS
    :type bbox: geometry.BBox
    :param start_date: beginning of time range
    :type start_date: datetime.date
    :param end_date: end of time range
    :type end_date: datetime.date
    :param absolute_orbit: An absolute orbit number of Sentinel-2 L1C products as defined by ESA
    :type absolute_orbit: int
    :return: dictionary with parameters as properties when arguments not None
    :rtype: dict
    """
    url_params = {
        'identifier': tile_id,
        'startDate': serialize_time(start_date, use_tz=False) if start_date else None,
        'completionDate': serialize_time(end_date, use_tz=False) if end_date else None,
        'orbitNumber': absolute_orbit,
        'box': bbox
    }
    return {key: str(value) for key, value in url_params.items() if value}

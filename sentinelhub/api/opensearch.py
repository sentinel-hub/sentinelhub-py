"""
Module for communication with Sentinel Hub Opensearch service.

For more search parameters check
`service description <http://opensearch.sentinel-hub.com/resto/api/collections/Sentinel2/describe.xml>`__.
"""
import datetime as dt
import logging
from typing import Iterable, Iterator, List, Optional, Union
from urllib.parse import urlencode

from ..config import SHConfig
from ..constants import CRS
from ..download import DownloadClient
from ..geometry import BBox
from ..time_utils import RawTimeIntervalType, RawTimeType, parse_time, parse_time_interval, serialize_time
from ..types import JsonDict

LOGGER = logging.getLogger(__name__)


class TileMissingException(Exception):
    """This exception is raised when requested tile is missing at Sentinel Hub Opensearch service"""


def get_tile_info_id(tile_id: str) -> JsonDict:
    """Get basic information about image tile

    :param tile_id: original tile identification string provided by ESA (e.g.
        'S2A_OPER_MSI_L1C_TL_SGS__20160109T230542_A002870_T10UEV_N02.01')
    :return: dictionary with info provided by Opensearch REST service
    :raises: TileMissingException if no tile with tile ID `tile_id` exists
    """
    result_list = list(search_iter(tile_id=tile_id))

    if not result_list:
        raise TileMissingException

    if len(result_list) > 1:
        LOGGER.warning("Obtained %d results for tile_id=%s. Returning the first one", len(result_list), tile_id)

    return result_list[0]


def get_tile_info(
    tile: str, time: Union[RawTimeType, RawTimeIntervalType], aws_index: Optional[int] = None, all_tiles: bool = False
) -> Union[JsonDict, List[JsonDict]]:
    """Get basic information about image tile

    :param tile: tile name (e.g. ``'T10UEV'``)
    :param time: A single date or a time interval
    :param aws_index: index of tile on AWS
    :param all_tiles: If `True` it will return list of all tiles otherwise only the first one
    :return: dictionary (or list of dictionaries) with info provided by Opensearch REST service
    """
    start_date, end_date = parse_time_interval(time)

    candidates = []
    for tile_info in search_iter(start_date=start_date, end_date=end_date):
        path_props = tile_info["properties"]["s3Path"].split("/")
        this_tile = "".join(path_props[1:4])
        this_aws_index = int(path_props[-1])
        if this_tile == tile.lstrip("T0") and (aws_index is None or aws_index == this_aws_index):
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
        LOGGER.info("Obtained more than one result for tile=%s, time=%s. Returning the first one", tile, time)
    return candidates[0]


def get_area_info(bbox: BBox, date_interval: RawTimeIntervalType, maxcc: Optional[float] = None) -> List[JsonDict]:
    """Get information about all images from specified area and time range

    :param bbox: bounding box of requested area
    :param date_interval: a pair of time strings in ISO8601 format
    :param maxcc: filter images by maximum percentage of cloud coverage
    :return: iterator of dictionaries containing info provided by Opensearch REST service
    """
    result_list = search_iter(bbox=bbox, start_date=date_interval[0], end_date=date_interval[1])
    if maxcc:
        return reduce_by_maxcc(result_list, maxcc)
    return list(result_list)


def get_area_dates(bbox: BBox, date_interval: RawTimeIntervalType, maxcc: Optional[float] = None) -> List[dt.date]:
    """Get list of times of existing images from specified area and time range

    :param bbox: bounding box of requested area
    :param date_interval: a pair of time strings in ISO8601 format
    :param maxcc: filter images by maximum percentage of cloud coverage
    :return: list of time strings in ISO8601 format
    """

    area_info = get_area_info(bbox, date_interval, maxcc=maxcc)
    return sorted({parse_time(tile_info["properties"]["startDate"]) for tile_info in area_info})


def reduce_by_maxcc(result_list: Iterable[JsonDict], maxcc: float) -> List[JsonDict]:
    """Filter list image tiles by maximum cloud coverage

    :param result_list: list of dictionaries containing info provided by Opensearch REST service
    :param maxcc: filter images by maximum percentage of cloud coverage
    :return: list of dictionaries containing info provided by Opensearch REST service
    """
    return [tile_info for tile_info in result_list if tile_info["properties"]["cloudCover"] <= 100 * float(maxcc)]


def search_iter(
    tile_id: Optional[str] = None,
    bbox: Optional[BBox] = None,
    start_date: Optional[RawTimeType] = None,
    end_date: Optional[RawTimeType] = None,
    absolute_orbit: Optional[int] = None,
    config: Optional[SHConfig] = None,
) -> Iterator[JsonDict]:
    """A generator function that implements OpenSearch search queries and returns results

    All parameters for search are optional.

    :param tile_id: original tile identification string provided by ESA (e.g.
                    'S2A_OPER_MSI_L1C_TL_SGS__20160109T230542_A002870_T10UEV_N02.01')
    :param bbox: bounding box of requested area
    :param start_date: beginning of time range
    :param end_date: end of time range
    :param absolute_orbit: An absolute orbit number of Sentinel-2 L1C products as defined by ESA
    :return: An iterator returning dictionaries with info provided by Sentinel Hub OpenSearch REST service
    :param config: A custom instance of config class to override parameters from the saved configuration.
    """
    config = config or SHConfig()

    if bbox and bbox.crs is not CRS.WGS84:
        bbox = bbox.transform(CRS.WGS84)

    start_date = parse_time(start_date) if start_date else None
    end_date = parse_time(end_date) if end_date else None

    url_params = _prepare_url_params(tile_id, bbox, end_date, start_date, absolute_orbit)
    url_params["maxRecords"] = config.max_opensearch_records_per_query

    start_index = 1
    client = DownloadClient(config=config)

    while True:
        url_params["index"] = start_index

        url = f"{config.opensearch_url}/search.json?{urlencode(url_params)}"
        LOGGER.debug("URL=%s", url)

        response = client.get_json_dict(url)
        yield from response["features"]

        if len(response["features"]) < config.max_opensearch_records_per_query:
            break
        start_index += config.max_opensearch_records_per_query


def _prepare_url_params(
    tile_id: Optional[str],
    bbox: Optional[BBox],
    end_date: Optional[dt.date],
    start_date: Optional[dt.date],
    absolute_orbit: Optional[int],
) -> JsonDict:
    """Constructs dict with URL params

    :param tile_id: original tile identification string provided by ESA (e.g.
                    'S2A_OPER_MSI_L1C_TL_SGS__20160109T230542_A002870_T10UEV_N02.01')
    :param bbox: bounding box of requested area in WGS84 CRS
    :param start_date: beginning of time range
    :param end_date: end of time range
    :param absolute_orbit: An absolute orbit number of Sentinel-2 L1C products as defined by ESA
    :return: dictionary with parameters as properties when arguments not None
    """
    url_params = {
        "identifier": tile_id,
        "startDate": serialize_time(start_date, use_tz=False) if start_date else None,
        "completionDate": serialize_time(end_date, use_tz=False) if end_date else None,
        "orbitNumber": absolute_orbit,
        "box": bbox,
    }
    return {key: str(value) for key, value in url_params.items() if value}

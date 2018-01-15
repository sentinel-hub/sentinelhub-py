"""
Module for manipulation of geographical information.
"""

# pylint: disable=unused-argument

import logging
import math
import pyproj

from .constants import CRS
from .common import BBox


LOGGER = logging.getLogger(__name__)


WGS84_PROJ = pyproj.Proj(init=CRS.ogc_string(CRS.WGS84))
EPSG3857_PROJ = pyproj.Proj(init=CRS.ogc_string(CRS.POP_WEB))

ERR = 0.1

# Semi-axes of WGS-84 geoidal reference
EARTH_WGS84_A = 6378137.0  # Major semiaxis [m]
EARTH_WGS84_B = 6356752.3  # Minor semiaxis [m]


def earth_radius_wgs84(lat):
    """ Returns earth's radius at a given latitude, according to the WGS-84 ellipsoid [m]

    :param lat: latitude in radians
    :type lat: float
    :return: Earth's radius at a given latitude
    :rtype: float

    """

    ad_lat = EARTH_WGS84_A * math.cos(lat)
    bd_lat = EARTH_WGS84_B * math.sin(lat)
    an_lat = EARTH_WGS84_A * ad_lat
    bn_lat = EARTH_WGS84_B * bd_lat

    return math.sqrt((an_lat*an_lat + bn_lat*bn_lat)/(ad_lat*ad_lat + bd_lat*bd_lat))


def bbox_to_resolution_wgs84(bbox, width, height):
    """ Calculates pixel resolution in meters for a given bbox, width and height.

    :param bbox: latitude and longitude of south/west and north/east points
    :type bbox: list of two tuples
    :param width: width of bounding box in pixels
    :type width: int
    :param height: height of bounding box in pixels
    :type height: int
    :return: resolution east-west at north and south, and resolution north-south in meters
    :rtype: float, float, float

    """

    x_mn, y_mn = bbox.get_lower_left()
    x_mx, y_mx = bbox.get_upper_right()

    res_ew_n = (math.radians(y_mx) - math.radians(y_mn)) * earth_radius_wgs84(
        math.radians(x_mn)) * math.cos(math.radians(x_mx))
    res_ew_s = (math.radians(y_mx) - math.radians(y_mn)) * earth_radius_wgs84(
        math.radians(x_mn)) * math.cos(math.radians(x_mn))
    res_ns = (math.radians(x_mx) - math.radians(x_mn)) * earth_radius_wgs84(math.radians(x_mn))

    return res_ew_n / width, res_ew_s / width, res_ns / height


def bbox_to_resolution(bbox, width, height):
    """ Calculates pixel resolution in meters for a given bbox of a given width and height in a given CRS.

    :param bbox: latitude and longitude of south/west and north/east points
    :type bbox: list of two tuples
    :param width: width of bounding box in pixels
    :type width: int
    :param height: height of bounding box in pixels
    :type height: int
    :return: resolution east-west at north and south, and resolution north-south in meters for given CRS
    :rtype: float, float, float
    :raises: ValueError if CRS is not supported
    """
    crs = bbox.get_crs()
    x_mn, y_mn = bbox.get_lower_left()
    x_mx, y_mx = bbox.get_upper_right()

    if crs is CRS.WGS84:
        return bbox_to_resolution_wgs84(bbox, width, height)
    elif crs is CRS.POP_WEB:
        bbox_wgs84 = BBox(
            [pyproj.transform(EPSG3857_PROJ, WGS84_PROJ, point[0], point[1]) for point in ((x_mn, y_mn), (x_mx, y_mx))],
            CRS.WGS84)
        return bbox_to_resolution_wgs84(bbox_wgs84, width, height)
    raise ValueError('Given CRS {} is not supported.'.format(crs))


def get_utm_bbox(img_bbox, transform):  # img_bbox = [row1, column1, row2, column2]
    """ Get UTM coordinates given a bounding box in pixels and a transform

    :param img_bbox: boundaries of bounding box in pixels as [row1, col1, row2, col2]
    :type img_bbox: list
    :param transform: georeferencing transform
    :type transform: list
    :return: UTM coordinates as [east1, north1, east2, north2]
    :rtype: list
    """
    east1, north1 = pixel_to_utm(img_bbox[0], img_bbox[1], transform)
    east2, north2 = pixel_to_utm(img_bbox[2], img_bbox[3], transform)
    return [east1, north1, east2, north2]


def wgs84_to_utm(lat, lng, utm_epsg):
    """ Convert WGS84 coordinates to UTM

    :param lat: latitude in WGS84 system
    :type lat: float
    :param lng: longitude in WGS84 system
    :type lng: float
    :param utm_epsg: UTM coordinate reference system enum constants
    :type utm_epsg: constants.CRS
    :return: east, north coordinates in UTM system
    :rtype: float, float
    """
    utm_proj = pyproj.Proj(init=CRS.ogc_string(utm_epsg))
    east, north = pyproj.transform(WGS84_PROJ, utm_proj, lng, lat)
    return east, north


def to_wgs84(east, north, epsg):
    """ Convert any CRS with (east, north) coordinates to WGS84

    :param east: east coordinate
    :type east: float
    :param north: north coordinate
    :type north: float
    :param epsg: CRS enum constants
    :type epsg: constants.CRS
    :return: latitude and longitude coordinates in WGS84 system
    :rtype: float, float
    """
    projection = pyproj.Proj(init=CRS.ogc_string(epsg))
    lng, lat = pyproj.transform(projection, WGS84_PROJ, east, north)
    return lat, lng


def utm_to_wgs84(east, north, utm_epsg):
    """ Convert UTM coordinates to WGS84

    :param east: east coordinate in UTM system
    :type east: float
    :param north: north coordinate in UTM system
    :type north: float
    :param utm_epsg: UTM coordinate reference system enum constants
    :type utm_epsg: constants.CRS
    :return: latitude and longitude coordinates in WGS84 system
    :rtype: float, float
    """
    return to_wgs84(east, north, utm_epsg)


def utm_to_pixel(east, north, transform, truncate=True):
    """ Convert UTM coordinate to image coordinate given a transform

    :param east: east coordinate of point
    :type east: float
    :param north: north coordinate of point
    :type north: float
    :param transform: georeferencing transform
    :type transform: list
    :param truncate: Whether to truncate pixel coordinates. Default is `True`
    :type truncate: bool
    :return: row and column pixel image coordinates
    :rtype: float, float or int, int
    """
    column = (east - transform[0]) / transform[1]
    row = (north - transform[3]) / transform[5]
    if truncate:
        return int(row + ERR), int(column + ERR)
    return row, column


def pixel_to_utm(row, column, transform):
    """ Convert pixel coordinate to UTM coordinate given a transform

    :param row: row pixel coordinate
    :type row: int or float
    :param column: column pixel coordinate
    :type column: int or float
    :param transform: georeferencing transform
    :type transform: list
    :return: east, north UTM coordinates
    :rtype: float, float
    """
    east = transform[0] + column * transform[1]
    north = transform[3] + row * transform[5]
    return east, north


def wgs84_to_pixel(lat, lng, utm_epsg, transform, truncate=True):
    """ Convert WGS84 coordinates to pixel image coordinates given a UTM CRS and a transform

    :param lat: latitude of point
    :type lat: float
    :param lng: longitude of point
    :type lng: float
    :param utm_epsg: UTM coordinate reference system enum constants
    :type utm_epsg: constants.CRS
    :param transform: georeferencing transform
    :type transform: list
    :param truncate: Whether to truncate pixel coordinates. Default is `True`
    :type truncate: bool
    :return: row and column pixel image coordinates
    :rtype: float, float or int, int
    """
    east, north = wgs84_to_utm(lat, lng, utm_epsg)
    row, column = utm_to_pixel(east, north, transform, truncate=truncate)
    return row, column


def get_utm_epsg_from_latlon(lat, lon):
    """ Get CRS for UTM zone in which (lat,lon) is contained.

    :param lat: latitude
    :type lat: float
    :param lon: longitude
    :type lon: float
    :return: CRS of the zone containing the lat,lon point
    :rtype: constants.CRS
    """
    return CRS.get_utm_from_wgs84(lat, lon)


def transform_point(point, src_crs, tht_crs):
    """ Maps point form src_crs to tgt_crs

    :param point: a tuple (x,y)
    :type point: tuple[float] of length 2
    :param src_crs: source CRS
    :type src_crs: constants.CRS
    :param tht_crs: target CRS
    :type tht_crs: constants.CRS
    :return: point in target CRS
    :rtype: tuple[float]
    """
    raise NotImplementedError


def transform_bbox(bbox, tgt_crs):
    """ Maps bbox from bbox.get_crs() to tgt_crs

    :param bbox: bounding box
    :type bbox: common.BBox
    :param tgt_crs: target CRS
    :type tgt_crs: constants.CRS
    :return: bounding box in target CRS
    :rtype: common.BBox
    """
    raise NotImplementedError

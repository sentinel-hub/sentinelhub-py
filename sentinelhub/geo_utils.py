"""
Module for manipulation of geographical information
"""
from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple, Union, cast

from .constants import CRS

if TYPE_CHECKING:
    from .geometry import BBox

ERR = 0.1


def bbox_to_dimensions(bbox: BBox, resolution: Union[float, Tuple[float, float]]) -> Tuple[int, int]:
    """Calculates width and height in pixels for a given bbox of a given pixel resolution (in meters). The result is
    rounded to the nearest integers.

    :param bbox: bounding box
    :param resolution: Resolution of desired image in meters. It can be a single number or a tuple of two numbers -
        resolution in horizontal and resolution in vertical direction.
    :return: width and height in pixels for given bounding box and pixel resolution
    """
    utm_bbox = to_utm_bbox(bbox)
    east1, north1 = utm_bbox.lower_left
    east2, north2 = utm_bbox.upper_right

    resx, resy = resolution if isinstance(resolution, tuple) else (resolution, resolution)

    return round(abs(east2 - east1) / resx), round(abs(north2 - north1) / resy)


def bbox_to_resolution(bbox: BBox, width: int, height: int, meters: bool = True) -> Tuple[float, float]:
    """Calculates pixel resolution for a given bbox of a given width and height. By default, it returns result in
    meters.

    :param bbox: bounding box
    :param width: width of bounding box in pixels
    :param height: height of bounding box in pixels
    :param meters: If `True` result will be given in meters, otherwise it will be given in units of current CRS
    :return: resolution east-west at north and south, and resolution north-south for given CRS
    :raises: ValueError if CRS is not supported
    """
    if meters:
        bbox = to_utm_bbox(bbox)
    east1, north1 = bbox.lower_left
    east2, north2 = bbox.upper_right
    return abs(east2 - east1) / width, abs(north2 - north1) / height


def get_image_dimension(bbox: BBox, width: Optional[int] = None, height: Optional[int] = None) -> int:
    """Given bounding box and one of the parameters width or height it will return the other parameter that will best
    fit the bounding box dimensions

    :param bbox: bounding box
    :param width: image width or `None` if height is unknown
    :param height: image height or `None` if height is unknown
    :return: width or height rounded to integer
    """
    utm_bbox = to_utm_bbox(bbox)
    east1, north1 = utm_bbox.lower_left
    east2, north2 = utm_bbox.upper_right
    if isinstance(width, int):
        return round(width * abs(north2 - north1) / abs(east2 - east1))
    if isinstance(height, int):
        return round(height * abs(east2 - east1) / abs(north2 - north1))
    raise ValueError("At least one of the parameters `width` and `height` must be given.")


def to_utm_bbox(bbox: BBox) -> BBox:
    """Transform bbox into UTM CRS

    :param bbox: bounding box
    :return: bounding box in UTM CRS
    """
    if CRS.is_utm(bbox.crs):
        return bbox
    lng, lat = bbox.middle
    utm_crs = get_utm_crs(lng, lat, source_crs=bbox.crs)
    return bbox.transform(utm_crs)


def get_utm_bbox(img_bbox: Sequence[float], transform: Sequence[float]) -> List[float]:
    """Get UTM coordinates given a bounding box in pixels and a transform

    :param img_bbox: boundaries of bounding box in pixels as `[row1, col1, row2, col2]`
    :param transform: georeferencing transform of the image, e.g. `(x_upper_left, res_x, 0, y_upper_left, 0, -res_y)`
    :return: UTM coordinates as [east1, north1, east2, north2]
    """
    east1, north1 = pixel_to_utm(img_bbox[0], img_bbox[1], transform)
    east2, north2 = pixel_to_utm(img_bbox[2], img_bbox[3], transform)
    return [east1, north1, east2, north2]


def wgs84_to_utm(lng: float, lat: float, utm_crs: Optional[CRS] = None) -> Tuple[float, float]:
    """Convert WGS84 coordinates to UTM. If UTM CRS is not set it will be calculated automatically.

    :param lng: longitude in WGS84 system
    :param lat: latitude in WGS84 system
    :param utm_crs: UTM coordinate reference system enum constants
    :return: east, north coordinates in UTM system
    """
    if utm_crs is None:
        utm_crs = get_utm_crs(lng, lat)
    return transform_point((lng, lat), CRS.WGS84, utm_crs)


def to_wgs84(east: float, north: float, crs: CRS) -> Tuple[float, float]:
    """Convert any CRS with (east, north) coordinates to WGS84

    :param east: east coordinate
    :param north: north coordinate
    :param crs: CRS enum constants
    :return: latitude and longitude coordinates in WGS84 system
    """
    return transform_point((east, north), crs, CRS.WGS84)


def utm_to_pixel(
    east: float, north: float, transform: Sequence[float], truncate: bool = True
) -> Union[Tuple[float, float], Tuple[int, int]]:
    """Convert a UTM coordinate to image coordinate given a transform

    :param east: east coordinate of point
    :param north: north coordinate of point
    :param transform: georeferencing transform of the image, e.g. `(x_upper_left, res_x, 0, y_upper_left, 0, -res_y)`
    :param truncate: Whether to truncate pixel coordinates. Default is `True`
    :return: row and column pixel image coordinates
    """
    column = (east - transform[0]) / transform[1]
    row = (north - transform[3]) / transform[5]
    if truncate:
        return int(row + ERR), int(column + ERR)
    return row, column


def pixel_to_utm(row: float, column: float, transform: Sequence[float]) -> Tuple[float, float]:
    """Convert pixel coordinate to UTM coordinate given a transform

    :param row: row pixel coordinate
    :param column: column pixel coordinate
    :param transform: georeferencing transform of the image, e.g. `(x_upper_left, res_x, 0, y_upper_left, 0, -res_y)`
    :return: east, north UTM coordinates
    """
    east = transform[0] + column * transform[1]
    north = transform[3] + row * transform[5]
    return east, north


def wgs84_to_pixel(
    lng: float, lat: float, transform: Sequence[float], utm_epsg: Optional[CRS] = None, truncate: bool = True
) -> Union[Tuple[float, float], Tuple[int, int]]:
    """Convert WGS84 coordinates to pixel image coordinates given transform and UTM CRS. If no CRS is given it will be
    calculated it automatically.

    :param lng: longitude of point
    :param lat: latitude of point
    :param transform: georeferencing transform of the image, e.g. `(x_upper_left, res_x, 0, y_upper_left, 0, -res_y)`
    :param utm_epsg: UTM coordinate reference system enum constants
    :param truncate: Whether to truncate pixel coordinates. Default is `True`
    :return: row and column pixel image coordinates
    """
    east, north = wgs84_to_utm(lng, lat, utm_epsg)
    row, column = utm_to_pixel(east, north, transform, truncate=truncate)
    return row, column


def get_utm_crs(lng: float, lat: float, source_crs: CRS = CRS.WGS84) -> CRS:
    """Get CRS for UTM zone in which (lat, lng) is contained.

    :param lng: longitude
    :param lat: latitude
    :param source_crs: source CRS
    :return: CRS of the zone containing the lat,lon point
    """
    if source_crs is not CRS.WGS84:
        lng, lat = transform_point((lng, lat), source_crs, CRS.WGS84)
    return CRS.get_utm_from_wgs84(lng, lat)


def transform_point(
    point: Tuple[float, float], source_crs: CRS, target_crs: CRS, always_xy: bool = True
) -> Tuple[float, float]:
    """Maps point form src_crs to tgt_crs

    :param point: a tuple `(x, y)`
    :param source_crs: source CRS
    :param target_crs: target CRS
    :param always_xy: Parameter that is passed to `pyproj.Transformer` object and defines axis order for
        transformation. The default value `True` is in most cases the correct one.
    :return: point in target CRS
    """
    if source_crs == target_crs:
        return point
    transform_function = CRS.get_transform_function(source_crs, target_crs, always_xy=always_xy)
    return cast(Tuple[float, float], transform_function(*point))

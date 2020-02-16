""" Helper functions for generating request dictionaries

Request structure documentation is available at: https://docs.sentinel-hub.com/api/latest/reference/
"""

from .geometry import Geometry


def body(request_bounds, request_data, evalscript, request_output=None):
    """ Generate request body
    """
    request_body = {
        "input": {
            "bounds": request_bounds,
            "data": request_data
        },
        "evalscript": evalscript
    }

    if request_output is not None:
        request_body['output'] = request_output

    return request_body


def response(identifier, response_format):
    """ Generate request response
    """
    return {
        "identifier": identifier,
        "format": {
            'type': response_format
        }
    }


def output(responses, size_x, size_y):
    """ Generate request output
    """
    return {
        "width": size_x,
        "height": size_y,
        "responses": responses
    }


def bounds(crs, bbox=None, geometry=None):
    """ Generate request bounds
    """
    if bbox is None and geometry is None:
        raise ValueError("At least one of parameters 'bbox' and 'geometry' has to be given")

    if bbox and (not isinstance(bbox, list) or len(bbox) != 4 or not all(isinstance(x, float) for x in bbox)):
        raise ValueError("Invalid bbox argument: {}".format(bbox))

    if geometry and not isinstance(geometry, Geometry):
        raise ValueError('Geometry has to be of type sentinelhub.Geometry')

    if bbox and geometry and bbox is not geometry.crs:
        raise ValueError('Bounding box and geometry should have the same CRS, but {} and {} '
                         'found'.format(bbox, geometry.crs))

    request_bounds = {
        "properties": {
            "crs": crs
        }
    }

    if bbox:
        request_bounds['bbox'] = list(bbox)

    if geometry:
        request_bounds['geometry'] = geometry.geojson

    return request_bounds


def data(time_from=None, time_to=None, data_type='S2L1C'):
    """ Generate request data
    """
    return {
        "type": data_type,
        "dataFilter": {
            "timeRange": {
                "from": "" if time_from is None else time_from,
                "to": "" if time_to is None else time_to
            }
        }
    }

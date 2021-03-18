""" SentinelHubRequest for the Process API

Documentation: https://docs.sentinel-hub.com/api/latest/reference/
"""
from .constants import MimeType, RequestType
from .data_collections import DataCollection, OrbitDirection, handle_deprecated_data_source
from .download import DownloadRequest, SentinelHubDownloadClient
from .data_request import DataRequest
from .geometry import Geometry, BBox
from .time_utils import parse_time_interval, serialize_time


class SentinelHubRequest(DataRequest):
    """ Sentinel Hub API request class
    """
    def __init__(self, evalscript, input_data, responses, bbox=None, geometry=None, size=None, resolution=None,
                 **kwargs):
        """
        For details of certain parameters check the
        `Process API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/process>`_.

        :param evalscript: `Evalscript <https://docs.sentinel-hub.com/api/latest/#/Evalscript/>`_.
        :type evalscript: str
        :param input_data: A list of input dictionary objects as described in the API reference. It can be generated
                           with the helper function `SentinelHubRequest.input_data`
        :type input_data: List[dict or InputDataDict]
        :param responses: A list of `output.responses` objects as described in the API reference. It can be generated
                           with the helper function `SentinelHubRequest.output_response`
        :type responses: List[dict]
        :param bbox: Bounding box describing the area of interest.
        :type bbox: sentinelhub.BBox
        :param geometry: Geometry describing the area of interest.
        :type geometry: sentinelhub.Geometry
        :param size: Size of the image.
        :type size: Tuple[int, int]
        :param resolution: Resolution of the image. It has to be in units compatible with the given CRS.
        :type resolution: Tuple[float, float]
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        if not isinstance(evalscript, str):
            raise ValueError("'evalscript' should be a string")

        parsed_mime_type = MimeType.from_string(responses[0]['format']['type'].split('/')[1])
        self.mime_type = MimeType.TAR if len(responses) > 1 else parsed_mime_type

        self.payload = self.body(
            request_bounds=self.bounds(bbox=bbox, geometry=geometry),
            request_data=input_data,
            request_output=self.output(size=size, resolution=resolution, responses=responses),
            evalscript=evalscript
        )

        super().__init__(SentinelHubDownloadClient, **kwargs)

    def create_request(self):
        """ Prepares a download request
        """
        headers = {'content-type': MimeType.JSON.get_string(), 'accept': self.mime_type.get_string()}

        self.download_list = [DownloadRequest(
            request_type=RequestType.POST,
            url=self._get_request_url(),
            post_values=self.payload,
            data_folder=self.data_folder,
            save_response=bool(self.data_folder),
            data_type=self.mime_type,
            headers=headers,
            use_session=True
        )]

    @staticmethod
    def input_data(data_collection=None, time_interval=None, maxcc=None, mosaicking_order=None, upsampling=None,
                   downsampling=None, other_args=None, data_source=None):
        """ Generate the `input` part of the Process API request body

        :param data_collection: One of supported Process API data collections.
        :type data_collection: DataCollection
        :param time_interval: interval with start and end date of the form YYYY-MM-DDThh:mm:ss or YYYY-MM-DD
        :type time_interval: (str, str) or (datetime, datetime)
        :param maxcc: Maximum accepted cloud coverage of an image. Float between 0.0 and 1.0. Default is 1.0.
        :type maxcc: float or None
        :param mosaicking_order: Mosaicking order, which has to be either 'mostRecent', 'leastRecent' or 'leastCC'.
        :type mosaicking_order: str or None
        :param upsampling: A type of upsampling to apply on data
        :type upsampling: str
        :param downsampling: A type of downsampling to apply on data
        :type downsampling: str
        :param other_args: Additional dictionary of arguments. If provided, the resulting dictionary will get updated
                           by it.
        :param other_args: dict
        :param data_source: A deprecated alternative of data_collection
        :type data_source: DataCollection
        :return: A dictionary-like object that also contains additional attributes
        :rtype: InputDataDict
        """
        data_collection = DataCollection(handle_deprecated_data_source(data_collection, data_source))
        input_data_dict = {
            'type': data_collection.api_id,
        }

        data_filters = _get_data_filters(data_collection, time_interval, maxcc, mosaicking_order)
        if data_filters:
            input_data_dict['dataFilter'] = data_filters

        processing_params = _get_processing_params(upsampling, downsampling)
        if processing_params:
            input_data_dict['processing'] = processing_params

        if other_args:
            _update_other_args(input_data_dict, other_args)

        return InputDataDict(input_data_dict, service_url=data_collection.service_url)

    @staticmethod
    def body(request_bounds, request_data, evalscript, request_output=None, other_args=None):
        """ Generate the body the Process API request body

        :param request_bounds: A dictionary as generated by `SentinelHubRequest.bounds` helper method.
        :type request_bounds: dict
        :param request_data: A list of dictionaries as generated by `SentinelHubRequest.input_data` helper method.
        :type request_data: List[dict]
        :param evalscript: Evalscript (https://docs.sentinel-hub.com/api/latest/#/Evalscript/)
        :type evalscript: str
        :param request_output: A dictionary as generated by `SentinelHubRequest.output` helper method.
        :type request_output: dict
        :param other_args: Additional dictionary of arguments. If provided, the resulting dictionary will get updated
                           by it.
        :param other_args: dict
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

        if other_args:
            _update_other_args(request_body, other_args)

        return request_body

    @staticmethod
    def output_response(identifier, response_format, other_args=None):
        """ Generate an element of `output.responses` as described in the Process API reference.

        :param identifier: Identifier of the output response.
        :type identifier: str
        :param response_format: A mime type of one of 'png', 'json', 'jpeg', 'tiff'.
        :type response_format: str or sentinelhub.MimeType
        :param other_args: Additional dictionary of arguments. If provided, the resulting dictionary will get updated
                           by it.
        :param other_args: dict
        """
        output_response = {
            "identifier": identifier,
            "format": {
                'type': MimeType(response_format).get_string()
            }
        }

        if other_args:
            _update_other_args(output_response, other_args)

        return output_response

    @staticmethod
    def output(responses, size=None, resolution=None, other_args=None):
        """ Generate an `output` part of the request as described in the Process API reference

        :param responses: A list of objects in `output.responses` as generated by `SentinelHubRequest.output_response`.
        :type responses: List[dict]
        :param size: Size of the image.
        :type size: Tuple[int, int]
        :param resolution: Resolution of the image. It has to be in units compatible with the given CRS.
        :type resolution: Tuple[float, float]
        :param other_args: Additional dictionary of arguments. If provided, the resulting dictionary will get updated
            by it.
        :param other_args: dict
        """
        if size and resolution:
            raise ValueError("Either size or resolution argument should be given, not both.")

        request_output = {
            "responses": responses
        }

        if size:
            request_output['width'], request_output['height'] = size
        if resolution:
            request_output['resx'], request_output['resy'] = resolution

        if other_args:
            _update_other_args(request_output, other_args)

        return request_output

    @staticmethod
    def bounds(bbox=None, geometry=None, other_args=None):
        """ Generate a `bound` part of the Process API request

        :param bbox: Bounding box describing the area of interest.
        :type bbox: sentinelhub.BBox
        :param geometry: Geometry describing the area of interest.
        :type geometry: sentinelhub.Geometry
        :param other_args: Additional dictionary of arguments. If provided, the resulting dictionary will get updated
                           by it.
        :param other_args: dict
        """
        if bbox is None and geometry is None:
            raise ValueError("'bbox' and/or 'geometry' have to be provided.")

        if bbox and not isinstance(bbox, BBox):
            raise ValueError("'bbox' should be an instance of sentinelhub.BBox")

        if geometry and not isinstance(geometry, Geometry):
            raise ValueError("'geometry' should be an instance of sentinelhub.Geometry")

        if bbox and geometry and bbox.crs != geometry.crs:
            raise ValueError("bbox and geometry should be in the same CRS")

        crs = bbox.crs if bbox else geometry.crs

        request_bounds = {
            "properties": {
                "crs": crs.opengis_string
            }
        }

        if bbox:
            request_bounds['bbox'] = list(bbox)

        if geometry:
            request_bounds['geometry'] = geometry.geojson

        if other_args:
            _update_other_args(request_bounds, other_args)

        return request_bounds

    def _get_request_url(self):
        """ It decides which service URL to query. Restrictions from data collection definitions overrule the
        settings from config object.
        """
        data_collection_urls = tuple({
            input_data_dict.service_url for input_data_dict in self.payload['input']['data']
            if isinstance(input_data_dict, InputDataDict) and input_data_dict.service_url is not None
        })
        if len(data_collection_urls) > 1:
            raise ValueError(f'Given data collections are restricted to different services: {data_collection_urls}\n'
                             f'Try defining data collections without these restrictions')

        base_url = data_collection_urls[0] if data_collection_urls else self.config.sh_base_url
        return f'{base_url}/api/v1/process'


class InputDataDict(dict):
    """ An input data dictionary which also holds additional attributes
    """
    def __init__(self, input_data_dict, *, service_url=None):
        """
        :param input_data_dict: A normal dictionary with input parameters
        :type input_data_dict: dict
        :param service_url: A service URL defined by a data collection
        :type service_url: str
        """
        super().__init__(input_data_dict)
        self.service_url = service_url

    def __repr__(self):
        """ Modified dictionary representation that also shows additional attributes
        """
        normal_dict_repr = super().__repr__()
        return f'{self.__class__.__name__}({normal_dict_repr}, service_url={self.service_url})'


def _get_data_filters(data_collection, time_interval, maxcc, mosaicking_order):
    """ Builds a dictionary of data filters for Process API
    """
    data_filter = {}

    if time_interval:
        start_time, end_time = serialize_time(parse_time_interval(time_interval, allow_undefined=True), use_tz=True)
        data_filter['timeRange'] = {'from': start_time, 'to': end_time}

    if maxcc is not None:
        if not isinstance(maxcc, float) and (maxcc < 0 or maxcc > 1):
            raise ValueError('maxcc should be a float on an interval [0, 1]')

        data_filter['maxCloudCoverage'] = int(maxcc * 100)

    if mosaicking_order:
        mosaic_order_params = ['mostRecent', 'leastRecent', 'leastCC']

        if mosaicking_order not in mosaic_order_params:
            raise ValueError(f'{mosaicking_order} is not a valid mosaickingOrder parameter, it should be one '
                             f'of: {mosaic_order_params}')

        data_filter['mosaickingOrder'] = mosaicking_order

    return {
        **data_filter,
        **_get_data_collection_filters(data_collection)
    }


def _get_data_collection_filters(data_collection):
    """ Builds a dictionary of filters for Process API from a data collection definition
    """
    filters = {}

    if data_collection.swath_mode:
        filters['acquisitionMode'] = data_collection.swath_mode.upper()

    if data_collection.polarization:
        filters['polarization'] = data_collection.polarization.upper()

    if data_collection.resolution:
        filters['resolution'] = data_collection.resolution.upper()

    if data_collection.orbit_direction and data_collection.orbit_direction.upper() != OrbitDirection.BOTH:
        filters['orbitDirection'] = data_collection.orbit_direction.upper()

    if data_collection.timeliness:
        filters['timeliness'] = data_collection.timeliness

    return filters


def _get_processing_params(upsampling, downsampling):
    """ Builds a dictionary of processing parameters for Process API
    """
    processing_params = {}

    if upsampling:
        processing_params['upsampling'] = upsampling

    if downsampling:
        processing_params['downsampling'] = downsampling

    return processing_params


def _update_other_args(dict1, dict2):
    """
    Function for a recursive update of `dict1` with `dict2`. The function loops over the keys in `dict2` and
    only the non-dict like values are assigned to the specified keys.
    """
    for key, value in dict2.items():
        if isinstance(value, dict) and key in dict1:
            _update_other_args(dict1[key], value)
        else:
            dict1[key] = value

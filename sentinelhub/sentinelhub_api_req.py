"""
Module implementing support for Sentinel Hub processing API
"""

from enum import Enum

import attr

from .config import SHConfig
from .constants import SHConstants, MimeType, DataSource, RequestType
from .download import DownloadRequest, SentinelHubDownloadClient
from .data_request import DataRequest
from .geometry import BBox, Geometry
from .time_utils import parse_time_interval


class ApiRequest(DataRequest):
    """ Sentinel Hub API request class

    TODO: some more explanation
    TODO: links to documentation
    """
    def __init__(self, evalscript, *, bbox=None, geometry=None, input_data=None, output_responses=None,
                 width=None, height=None, config=None, **kwargs):

        self.evalscript = evalscript

        self.bbox = bbox
        self.geometry = geometry
        self._check_geo_params()

        self.input_data = self._parse_input_data(input_data)
        self.output_responses = self._parse_output_responses(output_responses)

        self.width = width
        self.height = height
        if (self.width or self.height) and self.output_responses is not None and \
                not any(output_response.format_type.is_image_format() for output_response in self.output_responses):
            raise ValueError("Parameters 'width' and 'height' cannot be specified if none of outputs will be an image")

        self.config = config or SHConfig()

        self._download_request = None

        super().__init__(SentinelHubDownloadClient, **kwargs)

    def _check_geo_params(self):
        """ Method that checks if bbox and geometry parameters are ok
        """
        if self.bbox is None and self.geometry is None:
            raise ValueError("At least one of parameters 'bbox' and 'geometry' has to be given")

        if self.bbox and not isinstance(self.bbox, BBox):
            raise ValueError("'bbox' parameter expects an object of type sentinelhub.BBox, but {} "
                             "found".format(self.bbox))

        if self.geometry and not isinstance(self.geometry, Geometry):
            raise ValueError("'geometry' parameter expects an object of type sentinelhub.Geometry, but {} "
                             "found".format(self.geometry))

        if self.bbox and self.geometry and self.bbox.crs is not self.geometry.crs:
            raise ValueError('Bounding box and geometry should have the same CRS, but {} and {} '
                             'found'.format(self.bbox.crs, self.geometry.crs))

    @classmethod
    def _parse_input_data(cls, input_data):
        """ Parses multiple ways in which input_data can be given
        """
        if isinstance(input_data, list):
            return [cls._parse_single_input(input_params) for input_params in input_data]
        if isinstance(input_data, (ApiInputData, dict)):
            return [cls._parse_single_input(input_data)]
        raise ValueError("Parameter 'input_data' should be a list of sentinelhub.ApiInputData instances or"
                         "dictionaries with parameters")

    @staticmethod
    def _parse_single_input(input_params):
        """ Parses inputs for a single input datasource
        """
        if isinstance(input_params, ApiInputData):
            return input_params
        if not isinstance(input_params, dict):
            raise ValueError("Each element of 'input_params' list should either be an instance of "
                             "sentinelhub.ApiInputData or a dictionary or parameters")

        return ApiInputData(input_params)

    @classmethod
    def _parse_output_responses(cls, output_responses):
        """ Parses multiple ways in which output_responses can be given
        """
        if output_responses is None:
            return None
        if isinstance(output_responses, list):
            return [cls._parse_single_output_response(response) for response in output_responses]
        if isinstance(output_responses, (ApiOutputResponse, dict, MimeType)):
            return [cls._parse_single_output_response(output_responses)]
        raise ValueError("Parameter 'output_responses' should be a list of sentinelhub.ApiOutputResponse instances or"
                         "dictionaries with parameters")

    @classmethod
    def _parse_single_output_response(cls, output_response):
        """ Parses a single output response object
        """
        if isinstance(output_response, ApiOutputResponse):
            return output_response

        if isinstance(output_response, MimeType):
            return ApiOutputResponse(format_type=output_response)

        if not isinstance(output_response, dict):
            raise ValueError("Each element of 'output_responses' list should either be an instance of "
                             "sentinelhub.ApiOutputResponse or a dictionary of parameters or a MimeType")
        return ApiOutputResponse(
            identifier=output_response.get('identifier'),
            format_type=cls._collect_value(output_response, 'format_type', 'format', 'type'),
            format_quality=cls._collect_value(output_response, 'format_quality', 'format', 'quality')
        )

    @staticmethod
    def _collect_value(output_response, common_key, external_key, internal_key):
        """ Just a helper method for extracting values from inputs
        """
        for key in [common_key, internal_key]:
            if key in output_response:
                return output_response[key]
        if external_key not in output_response:
            return None
        return output_response[external_key].get(internal_key)

    def create_request(self):
        """ Prepares a download request
        """
        url = self.config.get_sh_processing_api_url()
        payload = self._create_payload()
        response_type = self._get_response_type()
        headers = {
            **SHConstants.HEADERS,
            "accept": response_type.get_string()
        }

        self._download_request = DownloadRequest(url=url, post_values=payload, hash_save=True,
                                                 data_folder=self.data_folder, data_type=response_type,
                                                 headers=SHConstants.HEADERS)

    @property
    def download_request(self):
        return self._download_request

    def get_data(self, save_data=False, redownload=False):
        """ TODO
        """
        self.download_list = [self._download_request]
        return super().get_data(save_data=save_data, redownload=redownload)[0]

    def save_data(self, redownload=False):
        """ TODO
        """
        self.download_list = [self._download_request]
        return super().save_data(redownload=redownload, max_threads=1)

    def _get_response_type(self):
        """ Get expected MimeType of the object returned by Sentinel Hub service
        """
        return MimeType.TAR
        # TODO: the following would work but would require different handling of the results
        # if len(self.output_responses) >= 2:
        #     return MimeType.TAR
        #
        # return self.output_responses[0].format_type

    def _create_payload(self):
        payload = {
            'input': {
                'bounds': self._create_bounds_payload(),
                'data': [single_input_data.get_payload() for single_input_data in self.input_data]
            },
            'evalscript': self.evalscript
        }

        if self.output_responses is not None:
            payload['output'] = self._create_output_payload()

        return payload

    def _create_bounds_payload(self):
        """ Creates bounds part of the API payload
        """
        crs = self.bbox.crs if self.bbox else self.geometry.crs
        bounds_payload = {
            'properties': {
                'crs': crs.opengis_string
            }
        }

        if self.bbox:
            bounds_payload['bbox'] = list(self.bbox)

        if self.geometry:
            bounds_payload['geometry'] = {key: value for key, value in self.geometry.geojson.items() if key != 'crs'}

        return bounds_payload

    def _create_output_payload(self):
        """ Generate request response
        """
        payload = {
            'responses': [response.get_payload() for response in self.output_responses]
        }
        if self.width:
            payload['width'] = self.width
        if self.height:
            payload['height'] = self.height

        return payload

# TODO: move to constants?
class DataSourceParam(Enum):

    TYPE = 'type'

    TIME_RANGE = 'timeRange'
    MOSAICKING_ORDER = 'mosaickingOrder'
    MAX_CLOUD_COVERAGE = 'maxCloudCoverage'
    PREVIEW_MODE = 'previewMode'

    RESOLUTION = 'resolution'
    ACQUISITION_MODE = 'acquisitionMode'
    POLARIZATION = 'polarization'
    ORBIT_DIRECTION = 'orbitDirection'

    UPSAMPLING = 'upsampling'
    DOWNSAMPLING = 'downsampling'
    ATMOSPHERIC_CORRECTION = 'atmosphericCorrection'

    CLAMP_NEGATIVE = 'clampNegative'
    EGM = 'egm'

    @classmethod
    def has_value(cls, value):
        """ Checks if value is in Enum class

        :return: True if value is one of the Enum constants and False otherwise
        :rtype: bool
        """
        return value in cls or value in cls._value2member_map_

    def is_filter(self):
        """ # TODO
        :return:
        """
        # TODO: maybe don't initialize this frozenset every time
        return self in frozenset([DataSourceParam.TIME_RANGE, DataSourceParam.MOSAICKING_ORDER,
                                  DataSourceParam.MAX_CLOUD_COVERAGE, DataSourceParam.PREVIEW_MODE,
                                  DataSourceParam.RESOLUTION, DataSourceParam.ACQUISITION_MODE,
                                  DataSourceParam.POLARIZATION, DataSourceParam.ORBIT_DIRECTION])

    def is_processing(self):
        """ TODO
        :return:
        """
        return self is not DataSourceParam.TYPE and not self.is_filter()

    def is_supported_by(self, data_source):
        pass  # TODO: only required for more detailed parameter checking


class ApiInputData:

    DATA_FILTER_PARAM = 'dataFilter'
    PROCESSING_PARAM = 'processing'

    def __init__(self, params):
        """ Checks right after init
        """
        # TODO: maybe raise an error or a warning if there are some unexpected parameters
        parsed_params = {key: value for key, value in params.items() if DataSourceParam.has_value(key)}

        for param_group_name in [self.DATA_FILTER_PARAM, self.PROCESSING_PARAM]:
            if param_group_name in params:
                if not isinstance(params[param_group_name], dict):
                    raise ValueError("Value of data source parameter '{}' should be a "
                                     "dictionary".format(param_group_name))
                parsed_params = {
                    **parsed_params,
                    **params[param_group_name]
                }
        self.params = {DataSourceParam(key): value for key, value in parsed_params.items()}

        if DataSourceParam.TYPE not in self.params:
            raise ValueError("Input data dictionary is missing an item 'type'")
        data_source_value = self.params[DataSourceParam.TYPE]
        if isinstance(data_source_value, str):
            self.params[DataSourceParam.TYPE] = DataSource.from_api_identifier(data_source_value)


    def get_payload(self):

        payload = {}

        for param_enum, value in self.params.items():

            if param_enum is DataSourceParam.TIME_RANGE and not isinstance(value, dict):
                time_interval = parse_time_interval(value)
                # TODO: from and to shouldn't be the same timestamp
                value = {
                    "from": '{}Z'.format(time_interval[0]), # TODO: because Z is required, maybe parse_time_interval could already return it
                    "to": '{}Z'.format(time_interval[1])
                }

            if param_enum is DataSourceParam.TYPE:
                payload[param_enum.value] = value.api_identifier()

            else:
                if param_enum.is_filter():
                    subdict_param = self.DATA_FILTER_PARAM
                else:
                    subdict_param = self.PROCESSING_PARAM

                insert_dict = payload.get(subdict_param, {})
                insert_dict[param_enum.value] = value
                payload[subdict_param] = insert_dict

        return payload


@attr.s(kw_only=True)
class ApiOutputResponse:

    format_type = attr.ib(converter=MimeType)
    identifier = attr.ib(default='default', validator=attr.validators.instance_of(str))
    format_quality = attr.ib(default=None, validator=attr.validators.optional(int))

    def __attrs_post_init__(self):
        """ Checks right after init
        """
        if self.format_quality is not None and self.format_type is not MimeType.JPG:
            raise ValueError('Output quality can only be specified for JPG outputs')

    def get_payload(self):
        payload = {
            'format': {
                'type': self.format_type.get_string()
            }
        }
        if self.identifier:
            payload['identifier'] = self.identifier

        if self.format_quality:
            payload['format']['quality'] = self.format_quality

        return payload

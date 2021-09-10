"""
Implementation of Sentinel Hub Process API interface
"""
from .constants import MimeType, RequestType
from .download import DownloadRequest
from .data_collections import OrbitDirection
from .data_request import DataRequest
from .geometry import Geometry, BBox
from .sh_utils import _update_other_args
from .time_utils import parse_time_interval, serialize_time


class SentinelHubBaseApiRequest(DataRequest):
    """ A base class for Sentinel Hub interfaces
    """
    _SERVICE_ENDPOINT = ''

    def create_request(self):
        """ Prepares a download request
        """
        headers = {'content-type': MimeType.JSON.get_string(), 'accept': self.mime_type.get_string()}
        base_url = self._get_base_url()
        self.download_list = [DownloadRequest(
            request_type=RequestType.POST,
            url=f'{base_url}/api/v1/{self._SERVICE_ENDPOINT}',
            post_values=self.payload,
            data_folder=self.data_folder,
            save_response=bool(self.data_folder),
            data_type=self.mime_type,
            headers=headers,
            use_session=True
        )]

    @staticmethod
    def input_data(data_collection, *, identifier=None, time_interval=None, maxcc=None, mosaicking_order=None,
                   upsampling=None, downsampling=None, other_args=None):
        """ Generate the `input data` part of the request body

        :param data_collection: One of supported Process API data collections.
        :type data_collection: DataCollection
        :param identifier: A collection identifier that can be referred to in the evalscript. Parameter is referenced
            as `"id"` in service documentation. To learn more check
            `data fusion documentation <https://docs.sentinel-hub.com/api/latest/data/data-fusion>`__.
        :type identifier: str or None
        :param time_interval: A time interval with start and end date of the form YYYY-MM-DDThh:mm:ss or YYYY-MM-DD or
            a datetime object
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
        :return: A dictionary-like object that also contains additional attributes
        :rtype: InputDataDict
        """
        input_data_dict = {
            'type': data_collection.api_id,
        }
        if identifier:
            input_data_dict['id'] = identifier

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
    def bounds(bbox=None, geometry=None, other_args=None):
        """ Generate a `bound` part of the API request

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
            raise ValueError('bbox and geometry should be in the same CRS')

        crs = bbox.crs if bbox else geometry.crs

        request_bounds = {
            'properties': {
                'crs': crs.opengis_string
            }
        }

        if bbox:
            request_bounds['bbox'] = list(bbox)

        if geometry:
            request_bounds['geometry'] = geometry.get_geojson(with_crs=False)

        if other_args:
            _update_other_args(request_bounds, other_args)

        return request_bounds

    def _get_base_url(self):
        """ It decides which base URL to use. Restrictions from data collection definitions overrule the
        settings from config object. In case different collections have different restrictions then
        `SHConfig.sh_base_url` breaks the tie in case it matches one of the data collection URLs.
        """
        data_collection_urls = tuple({
            input_data_dict.service_url.rstrip('/') for input_data_dict in self.payload['input']['data']
            if isinstance(input_data_dict, InputDataDict) and input_data_dict.service_url is not None
        })
        config_base_url = self.config.sh_base_url.rstrip('/')

        if not data_collection_urls:
            return config_base_url

        if len(data_collection_urls) == 1:
            return data_collection_urls[0]

        if config_base_url in data_collection_urls:
            return config_base_url

        raise ValueError(f'Given data collections are restricted to different services: {data_collection_urls}\n'
                         'Configuration parameter sh_base_url cannot break the tie because it is set to a different'
                         f'service: {config_base_url}')


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

    if data_collection.dem_instance:
        filters['demInstance'] = data_collection.dem_instance

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

"""
Module implementing an interface with Sentinel Hub Batch service
"""
from urllib.parse import urlencode

from .config import SHConfig
from .download.sentinelhub_client import get_sh_json
from .sentinelhub_request import SentinelHubRequest


class SentinelHubBatch:

    _REPR_PARAM_NAMES = ['id', 'description', 'bucketName', 'created', 'status', 'userAction']

    def __init__(self, request_id=None, *, request_info=None, config=None):
        if not (request_id or request_info):
            raise ValueError('One of the parameters request_id and request_info has to be given')

        self.request_id = request_id if request_id else request_info['id']
        self.config = config or SHConfig()
        self._request_info = request_info

    @classmethod
    def create(cls, sentinelhub_request, tiling_grid, output=None, bucket_name=None, description=None, config=None):

        if isinstance(sentinelhub_request, SentinelHubRequest):
            sentinelhub_request = sentinelhub_request.download_list[0].post_values
        if not isinstance(sentinelhub_request, dict):
            raise ValueError('Parameter sentinelhub_request should be an instance of SentinelHubRequest or a '
                             'dictionary of request payload')

        payload = {
            'processRequest': sentinelhub_request,
            'tilingGrid': tiling_grid,
            'output': output,
            'bucketName': bucket_name,
            'description': description
        }
        payload = _remove_undefined_params(payload)

        url = cls._get_process_url(config)
        request_info = get_sh_json(url, post_values=payload)
        return cls(request_info=request_info, config=config)

    @staticmethod
    def tiling_grid(grid_id, resolution, buffer=None):
        """
        TODO: maybe parse buffer?
        TODO: rename to build_tiling_grid ?
        """
        payload = {
            'id': grid_id,
            'resolution': resolution
        }
        if buffer:
            payload = {
                **payload,
                'bufferX': buffer[0],
                'bufferY': buffer[1]
            }
        return payload

    @staticmethod
    def output(*, default_tile_path=None, cog_output=None, cog_parameters=None, create_collection=None,
               collection_id=None, responses=None, **kwargs):
        """
        TODO: parse something else?
        """
        payload = {
            'defaultTilePath': default_tile_path,
            'cogOutput': cog_output,
            'cogParameters': cog_parameters,
            'createCollection': create_collection,
            'collectionId': collection_id,
            'responses': responses,
            **kwargs
        }
        return _remove_undefined_params(payload)

    @staticmethod
    def iter_tiling_grids(search=None, sort=None, config=None, **kwargs):
        """ An iterator over tiling grids

        :param search: A search parameter
        :type search: str
        :param sort: A sort parameter
        :type sort: str
        :param config: A configuration object
        :type config: SHConfig
        :param kwargs: Any other request query parameters
        """
        url = SentinelHubBatch._get_tiling_grids_url(config)
        params = _remove_undefined_params({
            'search': search,
            'sort': sort,
            **kwargs
        })
        return _iter_pages(url, **params)

    @staticmethod
    def get_tiling_grid(grid_id, config=None):
        """ Provides a single tiling grid

        :param grid_id: An ID of a requested tiling grid
        :type grid_id: str or int
        :param config: A configuration object
        :type config: SHConfig
        """
        url = f'{SentinelHubBatch._get_tiling_grids_url(config)}/{grid_id}'
        return get_sh_json(url)

    def __repr__(self):
        """ A representation that shows the basic parameters of a batch job
        """
        repr_params = {name: self.request_info[name] for name in self._REPR_PARAM_NAMES if name in self.request_info}
        repr_params_str = '\n  '.join(f'{name}: {value}' for name, value in repr_params.items())
        return f'{self.__class__.__name__}({{\n  {repr_params_str}\n  ...\n}})'

    @property
    def request_info(self):
        """ A dictionary with a Batch request information. It loads new only only if one doesn't exist yet.

        :return: Batch request info
        :rtype: dict
        """
        if self._request_info is None:
            self._update_request_info()
        return self._request_info

    def update_request_info(self):
        """ Updates and returns a dictionary with a Batch request information

        :return: Batch request info
        :rtype: dict
        """
        self._update_request_info()
        return self.request_info

    @property
    def evalscript(self):
        return self.request_info['processRequest']['evalscript']

    @property
    def geometry(self):
        pass

    @property
    def timestamp(self):
        pass

    @staticmethod
    def iter_requests(config=None, **params):
        """
        TODO: define params?
        TODO: cast to SentinelHubBatch?
        TODO: sorting?
        """
        url = SentinelHubBatch._get_process_url(config)
        for request_info in _iter_pages(url, **params):
            yield SentinelHubBatch(request_info=request_info, config=config)

    @staticmethod
    def get_latest_request(config=None):
        pass

    def start_analysis(self):
        self._call_job('analyse')

    def start_job(self):
        self._call_job('start')

    def cancel_job(self):
        self._call_job('cancel')

    def restart_job(self):
        self._call_job('restartpartial')

    def iter_tiles(self, **kwargs):
        url = self._get_tiles_url()
        return _iter_pages(url, **kwargs)

    def get_tile(self, tile_id):
        url = self._get_tiles_url(tile_id=tile_id)
        return get_sh_json(url)

    def reprocess_tile(self, tile_id):
        self._call_job(f'tiles/{tile_id}/restart')

    def _update_request_info(self):
        """ Collects new info about the current request
        """
        url = self._get_process_url(self.config, request_id=self.request_id)
        self._request_info = get_sh_json(url)

    def _call_job(self, endpoint_name):
        """ Makes a POST request to the service that triggers a processing job
        """
        process_url = self._get_process_url(request_id=self.request_id, config=self.config)
        url = f'{process_url}/{endpoint_name}'
        get_sh_json(url, post_values={}, return_data=False)

    def _get_tiles_url(self, tile_id=None):
        process_url = self._get_process_url(config=self.config, request_id=self.request_id)
        url = f'{process_url}/tiles'
        if tile_id:
            return f'{url}/{tile_id}'
        return url

    @staticmethod
    def _get_process_url(config, request_id=None):
        """ Creates an URL for process endpoint
        """
        url = f'{SentinelHubBatch._get_batch_url(config=config)}/process'
        if request_id:
            return f'{url}/{request_id}'
        return url

    @staticmethod
    def _get_tiling_grids_url(config):
        """ Creates an URL for tiling grids endpoint
        """
        return f'{SentinelHubBatch._get_batch_url(config=config)}/tilinggrids'

    @staticmethod
    def _get_batch_url(config=None):
        """
        TODO: maybe data source can change url
        """
        config = config or SHConfig()
        return f'{config.sh_base_url}/api/v1/batch'


def _remove_undefined_params(payload):
    """ Takes a dictionary with a payload and removes parameter which value is None
    """
    return {name: value for name, value in payload.items() if value is not None}


def _iter_pages(service_url, **params):
    """ Iterates over pages of items
    """
    token = None

    while True:
        if token is not None:
            params['viewtoken'] = token

        url = f'{service_url}?{urlencode(params)}'
        results = get_sh_json(url)

        for item in results['member']:
            yield item

        token = results['view'].get('nextToken')
        if token is None:
            break

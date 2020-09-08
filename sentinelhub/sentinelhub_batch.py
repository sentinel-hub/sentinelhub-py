"""
Module implementing an interface with Sentinel Hub Batch service
"""
from urllib.parse import urlencode

from .config import SHConfig
from .download.sentinelhub_client import get_sh_json
from .sentinelhub_request import SentinelHubRequest


class SentinelHubBatch:

    def __init__(self, request_id, config=None):

        self.request_id = request_id
        self.config = config or SHConfig()

        self._request_info = None

    @classmethod
    def create(cls, sentinelhub_request, tiling_grid, output=None, bucket_name=None, description=None, config=None):

        if isinstance(sentinelhub_request, SentinelHubRequest):
            sentinelhub_request = sentinelhub_request.download_list[0].post_values
        if not isinstance(sentinelhub_request, dict):
            raise ValueError('Parameter sentinelhub_request should be an instance of SentinelHubRequest or a '
                             'dictionary of request payload')

        payload = {
            'processRequest': sentinelhub_request,
            'tilingGrid': tiling_grid
        }
        for name, value in [('output', output),
                            ('bucketName', bucket_name),
                            ('description', description)]:
            if value is not None:
                payload[name] = value

        url = cls._get_process_url(config)
        request_info = get_sh_json(url, post_values=payload)
        request_id = request_info['id']

        batch_request = cls(request_id, config=config)
        batch_request._request_info = request_info
        return batch_request

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
        payload = {}
        for name, value in [('defaultTilePath', default_tile_path),
                            ('cogOutput', cog_output),
                            ('cogParameters', cog_parameters),
                            ('createCollection', create_collection),
                            ('collectionId', collection_id),
                            ('responses', responses)]:
            if value is not None:
                payload[name] = value
        return {
            **payload,
            **kwargs
        }

    def status(self):
        pass

    @staticmethod
    def iter_process_requests(config=None, **params):
        """
        TODO: define params?
        TODO: cast to SentinelHubBatch?
        TODO: sorting?
        """
        url = SentinelHubBatch._get_process_url(config)
        return _iter_pages(url, **params)  # TODO: parse geometries?

    @staticmethod
    def iter_tiling_grids(config=None, **params):
        """
        TODO: define params?
        """
        url = SentinelHubBatch._get_tiling_grids_url(config)
        return _iter_pages(url, **params)

    @staticmethod
    def get_tiling_grid(grid_id, config=None):
        url = f'{SentinelHubBatch._get_tiling_grids_url(config)}/{grid_id}'
        return get_sh_json(url)

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

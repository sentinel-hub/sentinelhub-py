"""
Module implementing an interface with Sentinel Hub Batch service
"""

from .config import SHConfig
from .constants import RequestType
from .download.sentinelhub_client import SentinelHubDownloadClient
from .geometry import Geometry, BBox, CRS
from .sentinelhub_request import SentinelHubRequest
from .sh_utils import SentinelHubFeatureIterator, remove_undefined


class SentinelHubBatch:
    """ An interface class for Sentinel Hub Batch API

    For more info check `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#tag/batch_process>`__.
    """
    _REPR_PARAM_NAMES = ['id', 'description', 'bucketName', 'created', 'status', 'userAction', 'valueEstimate',
                         'tileCount']

    def __init__(self, request_id=None, *, request_info=None, config=None):
        """
        :param request_id: A batch request ID
        :type request_id: str or None
        :param request_info: Information about batch request parameters obtained from the service. This parameter can
            be given instead of `request_id`
        :type request_info: dict or None
        :param config: A configuration object
        :type config: SHConfig or None
        """
        if not (request_id or request_info):
            raise ValueError('One of the parameters request_id and request_info has to be given')

        self.request_id = request_id if request_id else request_info['id']
        self.config = config or SHConfig()
        self._request_info = request_info

    def __repr__(self):
        """ A representation that shows the basic parameters of a batch job
        """
        repr_params = {name: self.info[name] for name in self._REPR_PARAM_NAMES if name in self.info}
        repr_params_str = '\n  '.join(f'{name}: {value}' for name, value in repr_params.items())
        return f'{self.__class__.__name__}({{\n  {repr_params_str}\n  ...\n}})'

    @classmethod
    def create(cls, sentinelhub_request, tiling_grid, output=None, bucket_name=None, description=None, config=None):
        """ Create a new batch request

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/createNewBatchProcessingRequest>`__

        :param sentinelhub_request: An instance of SentinelHubRequest class containing all request parameters.
            Alternatively, it can also be just a payload dictionary for Process API request
        :type sentinelhub_request: SentinelHubRequest or dict
        :param tiling_grid: A dictionary with tiling grid parameters. It can be built with `tiling_grid` method
        :type tiling_grid: dict
        :param output: A dictionary with output parameters. It can be built with `output` method. Alternatively, one
            can set `bucket_name` parameter instead.
        :type output: dict or None
        :param bucket_name: A name of an s3 bucket where to save data. Alternatively, one can set `output` parameter
            to specify more output parameters.
        :type bucket_name: str or None
        :param description: A description of a batch request
        :type description: str or None
        :param config: A configuration object
        :type config: SHConfig or None
        """
        if isinstance(sentinelhub_request, SentinelHubRequest):
            sentinelhub_request = sentinelhub_request.download_list[0].post_values

        if not isinstance(sentinelhub_request, dict):
            raise ValueError('Parameter sentinelhub_request should be an instance of SentinelHubRequest or a '
                             'dictionary with a request payload')

        payload = {
            'processRequest': sentinelhub_request,
            'tilingGrid': tiling_grid,
            'output': output,
            'bucketName': bucket_name,
            'description': description
        }
        payload = remove_undefined(payload)

        url = cls._get_process_url(config)
        client = SentinelHubDownloadClient(config=config)
        request_info = client.get_json(url, post_values=payload, use_session=True)

        return cls(request_info=request_info, config=config)

    @staticmethod
    def tiling_grid(grid_id, resolution, buffer=None, **kwargs):
        """ A helper method to build a dictionary with tiling grid parameters

        :param grid_id: An ID of a tiling grid
        :type grid_id: int
        :param resolution: A grid resolution
        :type resolution: float or int
        :param buffer: Optionally, a buffer around each tile can be defined. It can be defined with a tuple of integers
            `(buffer_x, buffer_y)`, which specifies a number of buffer pixels in horizontal and vertical directions.
        :type buffer: (int, int) or None
        :param kwargs: Any other arguments to be added to a dictionary of parameters
        :return: A dictionary with parameters
        :rtype: dict
        """
        payload = {
            'id': grid_id,
            'resolution': resolution,
            **kwargs
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
        """ A helper method to build a dictionary with tiling grid parameters

        :param default_tile_path: A path or a template on an s3 bucket where to store results. More info at Batch API
            documentation
        :type default_tile_path: str or None
        :param cog_output: A flag specifying if outputs should be written in COGs (cloud-optimized GeoTIFFs )or
            normal GeoTIFFs
        :type cog_output: bool or None
        :param cog_parameters: A dictionary specifying COG creation parameters
        :type cog_parameters: dict or None
        :param create_collection: If True the results will be written in COGs and a batch collection will be created
        :type create_collection: bool or None
        :param collection_id: If True results will be added to an existing collection
        :type collection_id: str or None
        :param responses: Specification of path template for individual outputs/responses
        :type responses: list or None
        :param kwargs: Any other arguments to be added to a dictionary of parameters
        :return: A dictionary of output parameters
        :rtype: dict
        """
        return remove_undefined({
            'defaultTilePath': default_tile_path,
            'cogOutput': cog_output,
            'cogParameters': cog_parameters,
            'createCollection': create_collection,
            'collectionId': collection_id,
            'responses': responses,
            **kwargs
        })

    @staticmethod
    def iter_tiling_grids(config=None, **kwargs):
        """ An iterator over tiling grids

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/getBatchTilingGridsProperties>`__

        :param config: A configuration object
        :type config: SHConfig
        :param kwargs: Any other request query parameters
        :return: An iterator over tiling grid definitions
        :rtype: Iterator[dict]
        """
        url = SentinelHubBatch._get_tiling_grids_url(config)
        return SentinelHubFeatureIterator(
            client=SentinelHubDownloadClient(config=config),
            url=url,
            params=remove_undefined(kwargs),
            exception_message='Failed to obtain information about available tiling grids'
        )

    @staticmethod
    def get_tiling_grid(grid_id, config=None):
        """ Provides a single tiling grid

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/getBatchTilingGridProperties>`__

        :param grid_id: An ID of a requested tiling grid
        :type grid_id: str or int
        :param config: A configuration object
        :type config: SHConfig
        :return: A tiling grid definition
        :rtype: dict
        """
        url = f'{SentinelHubBatch._get_tiling_grids_url(config)}/{grid_id}'
        client = SentinelHubDownloadClient(config=config)
        return client.get_json(url, use_session=True)

    @property
    def info(self):
        """ A dictionary with a Batch request information. It loads a new dictionary only if one doesn't exist yet.

        :return: Batch request info
        :rtype: dict
        """
        if self._request_info is None:
            self.update_info()
        return self._request_info

    def update_info(self):
        """ Updates information about a batch request

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/getSingleBatchProcessRequestById>`__

        :return: Batch request info
        :rtype: dict
        """
        url = self._get_process_url(self.config, request_id=self.request_id)
        client = SentinelHubDownloadClient(config=self.config)
        self._request_info = client.get_json(url, use_session=True)

    @property
    def evalscript(self):
        """ Provides an evalscript used by a batch request

        :return: An evalscript
        :rtype: str
        """
        return self.info['processRequest']['evalscript']

    @property
    def bbox(self):
        """Provides a bounding box used by a batch request

        :return: An area bounding box together with CRS
        :rtype: BBox
        :raises: ValueError
        """
        bbox, _, crs = self._parse_bounds_payload()
        if bbox is None:
            raise ValueError('Bounding box is not defined for this batch request')
        return BBox(bbox, crs)

    @property
    def geometry(self):
        """ Provides a geometry used by a batch request

        :return: An area geometry together with CRS
        :rtype: Geometry
        :raises: ValueError
        """
        _, geometry, crs = self._parse_bounds_payload()
        if geometry is None:
            raise ValueError('Geometry is not defined for this batch request')
        return Geometry(geometry, crs)

    @staticmethod
    def iter_requests(user_id=None, config=None, **kwargs):
        """ Iterate existing batch requests

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/getAllBathProcessRequests>`__

        :param user_id: Filter requests by a user id who defined a request
        :type user_id: str or None
        :param config: A configuration object
        :type config: SHConfig or None
        :param kwargs: Any additional parameters to include in a request query
        :return: An iterator over existing batch requests
        :rtype: Iterator[SentinelHubBatch]
        """
        url = SentinelHubBatch._get_process_url(config)
        params = remove_undefined({
            'userid': user_id,
            **kwargs
        })
        feature_iterator = SentinelHubFeatureIterator(
            client=SentinelHubDownloadClient(config=config),
            url=url,
            params=params,
            exception_message='No requests found'
        )
        for request_info in feature_iterator:
            yield SentinelHubBatch(request_info=request_info, config=config)

    @staticmethod
    def get_latest_request(config=None):
        """ Provides a batch request that has been created the latest
        """
        # This should be improved once sort parameter will be supported
        batch_requests = list(SentinelHubBatch.iter_requests(config=config))
        return max(*batch_requests, key=lambda request: request.info['created'])

    def delete(self):
        """ Delete a batch job request

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/deleteBatchProcessRequest>`__
        """
        url = self._get_process_url(self.config, request_id=self.request_id)
        client = SentinelHubDownloadClient(config=self.config)
        return client.get_json(url, request_type=RequestType.DELETE, use_session=True)

    def start_analysis(self):
        """ Starts analysis of a batch job request

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/batchAnalyse>`__
        """
        return self._call_job('analyse')

    def start_job(self):
        """ Starts running a batch job

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/batchStartProcessRequest>`__
        """
        return self._call_job('start')

    def cancel_job(self):
        """ Cancels a batch job

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/batchCancelProcessRequest>`__
        """
        return self._call_job('cancel')

    def restart_job(self):
        """ Restarts only those parts of a job that failed

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/batchRestartPartialProcessRequest>`__
        """
        return self._call_job('restartpartial')

    def iter_tiles(self, status=None, **kwargs):
        """ Iterate over info about batch request tiles

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getAllBatchProcessTiles>`__

        :param status: A filter to obtain only tiles with a certain status
        :type status: str or None
        :param kwargs: Any additional parameters to include in a request query
        :return: An iterator over information about each tile
        :rtype: Iterator[dict]
        """
        url = self._get_tiles_url()
        params = remove_undefined({
            'status': status,
            **kwargs
        })
        return SentinelHubFeatureIterator(
            client=SentinelHubDownloadClient(config=self.config),
            url=url,
            params=params,
            exception_message='No tiles found, please run analysis on batch request before calling this method'
        )

    def get_tile(self, tile_id):
        """ Provides information about a single batch request tile

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getBatchTileById>`__

        :param tile_id: An ID of a tile
        :type tile_id: int or None
        :return: Information about a tile
        :rtype: dict
        """
        url = self._get_tiles_url(tile_id=tile_id)
        client = SentinelHubDownloadClient(config=self.config)
        return client.get_json(url, use_session=True)

    def reprocess_tile(self, tile_id):
        """ Reprocess a single failed tile

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/restartBatchTileById>`__

        :param tile_id: An ID of a tile
        :type tile_id: int or None
        """
        self._call_job(f'tiles/{tile_id}/restart')

    def _parse_bounds_payload(self):
        """ Parses bbox, geometry and crs from batch request payload. If bbox or geometry don't exist it returns None
        instead.
        """
        bounds_definition = self.info['processRequest']['input']['bounds']
        crs = CRS(bounds_definition['properties']['crs'].rsplit('/', 1)[-1])

        return bounds_definition.get('bbox'), bounds_definition.get('geometry'), crs

    def _call_job(self, endpoint_name):
        """ Makes a POST request to the service that triggers a processing job
        """
        process_url = self._get_process_url(request_id=self.request_id, config=self.config)
        url = f'{process_url}/{endpoint_name}'

        client = SentinelHubDownloadClient(config=self.config)
        return client.get_json(url, request_type=RequestType.POST, use_session=True)

    def _get_tiles_url(self, tile_id=None):
        """ Creates an URL for tiles endpoint
        """
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
        """ Creates an URL of the base batch service
        """
        config = config or SHConfig()
        return f'{config.sh_base_url}/api/v1/batch'

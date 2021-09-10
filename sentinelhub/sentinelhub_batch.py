"""
Module implementing an interface with Sentinel Hub Batch service
"""
import logging
import time
import datetime as dt
from collections import defaultdict
from dataclasses import field, dataclass
from enum import Enum
from typing import Optional

from dataclasses_json import config as dataclass_config
from dataclasses_json import dataclass_json, LetterCase, Undefined, CatchAll
from tqdm.auto import tqdm

from .constants import RequestType
from .data_collections import DataCollection
from .geometry import Geometry, BBox, CRS
from .sentinelhub_request import SentinelHubRequest
from .sh_utils import (
    SentinelHubService, SentinelHubFeatureIterator, remove_undefined, BaseCollection, datetime_config, enum_config
)

LOGGER = logging.getLogger(__name__)


class SentinelHubBatch(SentinelHubService):
    """ An interface class for Sentinel Hub Batch API

    For more info check `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#tag/batch_process>`__.
    """
    # pylint: disable=too-many-public-methods
    @staticmethod
    def _get_service_url(base_url):
        """ Provides URL to Catalog API
        """
        return f'{base_url}/api/v1/batch'

    def create(self, sentinelhub_request, tiling_grid, output=None, bucket_name=None, description=None, **kwargs):
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
        :param kwargs: Any other arguments to be added to a dictionary of parameters.
        :return: An instance of `SentinelHubBatch` object that represents a newly created batch request.
        :rtype: BatchRequest
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
            'description': description,
            **kwargs
        }
        payload = remove_undefined(payload)

        url = self._get_process_url()
        request_info = self.client.get_json(url, post_values=payload, use_session=True)

        return BatchRequest.from_dict(request_info)

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
    def output(*, default_tile_path=None, overwrite=None, skip_existing=None, cog_output=None, cog_parameters=None,
               create_collection=None, collection_id=None, responses=None, **kwargs):
        """ A helper method to build a dictionary with tiling grid parameters

        :param default_tile_path: A path or a template on an s3 bucket where to store results. More info at Batch API
            documentation
        :type default_tile_path: str or None
        :param overwrite: A flag specifying if a request should overwrite existing outputs without failing
        :type overwrite: bool or None
        :param skip_existing: A flag specifying if existing outputs should be overwritten
        :type skip_existing: bool or None
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
            'overwrite': overwrite,
            'skipExisting': skip_existing,
            'cogOutput': cog_output,
            'cogParameters': cog_parameters,
            'createCollection': create_collection,
            'collectionId': collection_id,
            'responses': responses,
            **kwargs
        })

    def iter_tiling_grids(self, **kwargs):
        """ An iterator over tiling grids

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/getBatchTilingGridsProperties>`__

        :param kwargs: Any other request query parameters
        :return: An iterator over tiling grid definitions
        :rtype: Iterator[dict]
        """
        return SentinelHubFeatureIterator(
            client=self.client,
            url=self._get_tiling_grids_url(),
            params=remove_undefined(kwargs),
            exception_message='Failed to obtain information about available tiling grids'
        )

    def get_tiling_grid(self, grid_id):
        """ Provides a single tiling grid

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/getBatchTilingGridProperties>`__

        :param grid_id: An ID of a requested tiling grid
        :type grid_id: str or int
        :return: A tiling grid definition
        :rtype: dict
        """
        return self.client.get_json(
            self._get_tiling_grids_url(grid_id),
            use_session=True
        )

    def iter_requests(self, user_id=None, search=None, sort=None, **kwargs):
        """ Iterate existing batch requests

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/getAllBatchProcessRequests>`__

        :param user_id: Filter requests by a user id who defined a request
        :type user_id: str or None
        :param search: A search query to filter requests
        :type search: str or None
        :param sort: A sort query
        :type sort: str or None
        :param kwargs: Any additional parameters to include in a request query
        :return: An iterator over existing batch requests
        :rtype: Iterator[BatchRequest]
        """
        params = remove_undefined({
            'userid': user_id,
            'search': search,
            'sort': sort,
            **kwargs
        })
        feature_iterator = SentinelHubFeatureIterator(
            client=self.client,
            url=self._get_process_url(),
            params=params,
            exception_message='No requests found'
        )
        for request_info in feature_iterator:
            yield BatchRequest.from_dict(request_info)

    def get_latest_request(self):
        """ Provides a batch request that has been created the latest

        :return: Batch request info
        :rtype: BatchRequest
        """
        latest_request_iter = self.iter_requests(sort='created:desc', count=1)
        try:
            return next(latest_request_iter)
        except StopIteration as exception:
            raise ValueError('No batch request is available') from exception

    def get_request(self, batch_request):
        """ Collects information about a single batch request

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/getSingleBatchProcessRequestById>`__

        :return: Batch request info
        :rtype: BatchRequest
        """
        request_id = self._parse_request_id(batch_request)
        request_info = self.client.get_json(
            url=self._get_process_url(request_id),
            use_session=True
        )
        return BatchRequest.from_dict(request_info)

    def update_request(self, batch_request, output=None, description=None, **kwargs):
        """ Update batch job request parameters

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/updateBatchProcessRequest>`__

        Similarly to `update_info` method, this method also updates local information in the current instance of
        `SentinelHubBatch`.

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        :type batch_request: BatchRequest or dict or str
        :param output: A dictionary with output parameters to be updated.
        :type output: dict or None
        :param description: A description of a batch request to be updated.
        :type description: str or None
        :param kwargs: Any other arguments to be added to a dictionary of parameters.
        """
        request_id = self._parse_request_id(batch_request)
        payload = remove_undefined({
            'output': output,
            'description': description,
            **kwargs
        })
        return self.client.get_json(
            url=self._get_process_url(request_id),
            post_values=payload,
            request_type=RequestType.PUT,
            use_session=True
        )

    def delete_request(self, batch_request):
        """ Delete a batch job request

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/deleteBatchProcessRequest>`__

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        :type batch_request: BatchRequest or dict or str
        """
        request_id = self._parse_request_id(batch_request)
        return self.client.get_json(
            url=self._get_process_url(request_id),
            request_type=RequestType.DELETE,
            use_session=True
        )

    def start_analysis(self, batch_request):
        """ Starts analysis of a batch job request

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/batchAnalyse>`__

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        :type batch_request: BatchRequest or dict or str
        """
        return self._call_job(batch_request, 'analyse')

    def start_job(self, batch_request):
        """ Starts running a batch job

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/batchStartProcessRequest>`__

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        :type batch_request: BatchRequest or dict or str
        """
        return self._call_job(batch_request, 'start')

    def cancel_job(self, batch_request):
        """ Cancels a batch job

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/batchCancelProcessRequest>`__

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        :type batch_request: BatchRequest or dict or str
        """
        return self._call_job(batch_request, 'cancel')

    def restart_job(self, batch_request):
        """ Restarts only those parts of a job that failed

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/batchRestartPartialProcessRequest>`__

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        :type batch_request: BatchRequest or dict or str
        """
        return self._call_job(batch_request, 'restartpartial')

    def iter_tiles(self, batch_request, status=None, **kwargs):
        """ Iterate over info about batch request tiles

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getAllBatchProcessTiles>`__

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        :type batch_request: BatchRequest or dict or str
        :param status: A filter to obtain only tiles with a certain status
        :type status: BatchTileStatus or str or None
        :param kwargs: Any additional parameters to include in a request query
        :return: An iterator over information about each tile
        :rtype: Iterator[dict]
        """
        request_id = self._parse_request_id(batch_request)
        if isinstance(status, BatchTileStatus):
            status = status.value

        return SentinelHubFeatureIterator(
            client=self.client,
            url=self._get_tiles_url(request_id),
            params={'status': status, **kwargs},
            exception_message='No tiles found, please run analysis on batch request before calling this method'
        )

    def get_tile(self, batch_request, tile_id):
        """ Provides information about a single batch request tile

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getBatchTileById>`__

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        :type batch_request: BatchRequest or dict or str
        :param tile_id: An ID of a tile
        :type tile_id: int or None
        :return: Information about a tile
        :rtype: dict
        """
        request_id = self._parse_request_id(batch_request)
        url = self._get_tiles_url(request_id, tile_id=tile_id)
        return self.client.get_json(url, use_session=True)

    def reprocess_tile(self, batch_request, tile_id):
        """ Reprocess a single failed tile

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/restartBatchTileById>`__

        :param batch_request: It could be a batch request object, a raw batch request payload or only a batch
            request ID.
        :type batch_request: BatchRequest or dict or str
        :param tile_id: An ID of a tile
        :type tile_id: int or None
        """
        self._call_job(batch_request, f'tiles/{tile_id}/restart')

    def iter_collections(self, search=None, **kwargs):
        """ Iterate over batch collections

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/getAllBatchCollections>`__

        :param search: A search query to filter collections
        :type search: str or None
        :param kwargs: Any additional parameters to include in a request query
        :return: An iterator over existing batch collections
        :rtype: Iterator[dict]
        """
        return SentinelHubFeatureIterator(
            client=self.client,
            url=self._get_collections_url(),
            params={'search': search, **kwargs},
            exception_message='Failed to obtain information about available Batch collections'
        )

    def get_collection(self, collection_id):
        """ Get batch collection by its id

        `Batch API reference
        <https://docs.sentinel-hub.com/api/latest/reference/#operation/getSingleBatchCollectionById>`__

        :param collection_id: A batch collection id
        :type collection_id: str
        :return: A dictionary of the collection parameters
        :rtype: dict
        """
        return self.client.get_json(
            url=self._get_collections_url(collection_id),
            use_session=True
        )['data']

    def create_collection(self, collection):
        """ Create a new batch collection

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/createNewBatchCollection>`__

        :param collection: Batch collection definition
        :type collection: BatchCollection or dict
        :return: A dictionary of a newly created collection
        :rtype: dict
        """
        collection_payload = self._parse_collection_to_dict(collection)
        return self.client.get_json(
            url=self._get_collections_url(),
            post_values=collection_payload,
            use_session=True
        )['data']

    def update_collection(self, collection):
        """ Update an existing batch collection

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/updateBatchCollection>`__

        :param collection: Batch collection definition
        :type collection: BatchCollection or dict
        """
        collection_id = self._parse_collection_id(collection)
        return self.client.get_json(
            url=self._get_collections_url(collection_id),
            post_values=self._parse_collection_to_dict(collection),
            request_type=RequestType.PUT,
            use_session=True
        )

    def delete_collection(self, collection):
        """ Delete an existing batch collection

        `Batch API reference <https://docs.sentinel-hub.com/api/latest/reference/#operation/deleteBatchCollection>`__

        :param collection: Batch collection id or object
        :type collection: str or BatchCollection
        """
        collection_id = self._parse_collection_id(collection)
        return self.client.get_json(
            url=self._get_collections_url(collection_id),
            request_type=RequestType.DELETE,
            use_session=True
        )

    def _call_job(self, batch_request, endpoint_name):
        """ Makes a POST request to the service that triggers a processing job
        """
        request_id = self._parse_request_id(batch_request)
        job_url = f'{self._get_process_url(request_id)}/{endpoint_name}'
        return self.client.get_json(
            url=job_url,
            request_type=RequestType.POST,
            use_session=True
        )

    def _get_process_url(self, request_id=None):
        """ Creates an URL for process endpoint
        """
        url = f'{self.service_url}/process'
        if request_id:
            return f'{url}/{request_id}'
        return url

    def _get_tiles_url(self, request_id, tile_id=None):
        """ Creates an URL for tiles endpoint
        """
        url = f'{self._get_process_url(request_id)}/tiles'
        if tile_id:
            return f'{url}/{tile_id}'
        return url

    def _get_tiling_grids_url(self, grid_id=None):
        """ Creates an URL for tiling grids endpoint
        """
        url = f'{self.service_url}/tilinggrids'
        if grid_id:
            return f'{url}/{grid_id}'
        return url

    def _get_collections_url(self, collection_id=None):
        """ Creates an URL for batch collections endpoint
        """
        url = f'{self.service_url}/collections'
        if collection_id:
            return f'{url}/{collection_id}'
        return url

    @staticmethod
    def _parse_request_id(data):
        """ Parses batch request id from multiple possible inputs
        """
        if isinstance(data, BatchRequest):
            return data.request_id
        if isinstance(data, dict):
            return data['id']
        if isinstance(data, str):
            return data
        raise ValueError(f'Expected a BatchRequest, dictionary or a string, got {data}.')

    @staticmethod
    def _parse_collection_id(data):
        """ Parses batch collection id from multiple possible inputs
        """
        if isinstance(data, (BatchCollection, DataCollection)):
            return data.collection_id
        if isinstance(data, dict):
            return data['id']
        if isinstance(data, str):
            return data
        raise ValueError(f'Expected a BatchCollection dataclass, dictionary or a string, got {data}.')

    @staticmethod
    def _parse_collection_to_dict(data):
        """ Constructs a dictionary from given object
        """
        if isinstance(data, BatchCollection):
            return data.to_dict()
        if isinstance(data, dict):
            return data
        raise ValueError(f'Expected either a BatchCollection or a dict, got {data}.')


class BatchRequestStatus(Enum):
    """ An enum class with all possible batch request statuses
    """
    CREATED = 'CREATED'
    ANALYSING = 'ANALYSING'
    ANALYSIS_DONE = 'ANALYSIS_DONE'
    PROCESSING = 'PROCESSING'
    DONE = 'DONE'
    FAILED = 'FAILED'
    PARTIAL = 'PARTIAL'
    CANCELED = 'CANCELED'


class BatchTileStatus(Enum):
    """ An enum class with all possible batch tile statuses
    """
    PENDING = 'PENDING'
    SCHEDULED = 'SCHEDULED'
    QUEUED = 'QUEUED'
    PROCESSING = 'PROCESSING'
    PROCESSED = 'PROCESSED'
    FAILED = 'FAILED'


class BatchUserAction(Enum):
    """ An enum class with all possible batch user actions
    """
    START = 'START'
    ANALYSE = 'ANALYSE'
    NONE = 'NONE'
    CANCEL = 'CANCEL'


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass
class BatchRequest:
    """ A dataclass object that holds information about a batch request
    """
    request_id: str = field(metadata=dataclass_config(field_name='id'))
    process_request: dict
    other_data: CatchAll
    user_id: Optional[str] = None
    created: Optional[dt.datetime] = field(metadata=datetime_config, default=None)
    tiling_grid: Optional[dict] = None
    output: Optional[dict] = None
    bucket_name: Optional[str] = None
    description: Optional[str] = None
    value_estimate: Optional[float] = None
    tile_count: Optional[int] = None
    tile_width_px: Optional[int] = None
    tile_height_px: Optional[int] = None
    user_action: Optional[BatchUserAction] = field(metadata=enum_config(BatchUserAction), default=None)
    user_action_updated: Optional[str] = field(metadata=datetime_config, default=None)
    status: Optional[BatchRequestStatus] = field(metadata=enum_config(BatchRequestStatus), default=None)
    error: Optional[str] = None

    _REPR_PARAM_NAMES = ['request_id', 'description', 'bucket_name', 'created', 'status', 'user_action',
                         'value_estimate', 'tile_count']

    def __repr__(self):
        """ A representation that shows the basic parameters of a batch job
        """
        repr_params = {name: getattr(self, name) for name in self._REPR_PARAM_NAMES if getattr(self, name) is not None}
        repr_params_str = '\n  '.join(f'{name}={value}' for name, value in repr_params.items())
        return f'{self.__class__.__name__}(\n  {repr_params_str}\n  ...\n)'

    @property
    def evalscript(self):
        """ Provides an evalscript used by a batch request

        :return: An evalscript
        :rtype: str
        """
        return self.process_request['evalscript']

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

    def raise_for_status(self, status=BatchRequestStatus.FAILED):
        """ Raises an error in case batch request has a given status

        :param status: One or more status codes on which to raise an error. The default is `'FAILED'`.
        :type status: str or list(str) or BatchRequestStatus or list(BatchRequestStatus)
        :raises: RuntimeError
        """
        if isinstance(status, (str, BatchRequestStatus)):
            status = [status]
        status_list = [BatchRequestStatus(_status) for _status in status]

        if self.status in status_list:
            formatted_error_message = f' and error message: "{self.error}"' if self.error else ''
            raise RuntimeError(f'Raised for batch request {self.request_id} with status {self.status.value}'
                               f'{formatted_error_message}')

    def _parse_bounds_payload(self):
        """ Parses bbox, geometry and crs from batch request payload. If bbox or geometry don't exist it returns None
        instead.
        """
        bounds_definition = self.process_request['input']['bounds']
        crs = CRS(bounds_definition['properties']['crs'].rsplit('/', 1)[-1])

        return bounds_definition.get('bbox'), bounds_definition.get('geometry'), crs


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass
class BatchCollectionBatchData:
    """ Dataclass to hold batch collection batchData part of the payload
    """
    other_data: CatchAll
    tiling_grid_id: Optional[int] = None


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.INCLUDE)
@dataclass
class BatchCollectionAdditionalData:
    """ Dataclass to hold batch collection additionalData part of the payload
    """
    other_data: CatchAll
    bands: Optional[dict] = None


class BatchCollection(BaseCollection):
    """ Dataclass for batch collections
    """
    batch_data: Optional[BatchCollectionBatchData] = None
    additional_data: Optional[BatchCollectionAdditionalData] = None


_MIN_SLEEP_TIME = 60
_MIN_ANALYSIS_SLEEP_TIME = 5


def monitor_batch_job(batch_request, config, sleep_time=120, analysis_sleep_time=10):
    """ A utility function that keeps checking for number of processed tiles until the given batch request finishes.
    During the process it shows a progress bar and at the end it reports information about finished and failed tiles.

    Notes:

      - Before calling this function make sure to start a batch job by calling `SentinelHubBatch.start_job` method. In
        case a batch job is still being analysed this function will wait until the analysis ends.
      - This function will be continuously collecting tile information from Sentinel Hub service. To avoid making too
        many requests please make sure to adjust `sleep_time` parameter according to the size of your job. Larger jobs
        don't need too frequent tile status updates.
      - Some information about the progress of this function is reported to logging level INFO.

    :param batch_request: An object with information about a batch request. Alternatively, it could only be a batch
        request id or a payload.
    :type batch_request: BatchRequest or dict or str.
    :param config: A configuration object with required parameters `sh_client_id`, `sh_client_secret`, and
        `sh_auth_base_url` which is used for authentication and `sh_base_url` which defines the service deployment
        where Batch API will be called.
    :type config: SHConfig or None
    :param sleep_time: Number of seconds to sleep between consecutive progress bar updates.
    :type sleep_time: int
    :param analysis_sleep_time: Number of seconds between consecutive status updates during analysis phase.
    :type analysis_sleep_time: int
    :return: A dictionary mapping a tile status to a list of tile payloads.
    :rtype: defaultdict(BatchTileStatus, list(dict))
    """
    if sleep_time < _MIN_SLEEP_TIME:
        raise ValueError(f'To avoid making too many service requests please set sleep_time>={_MIN_SLEEP_TIME}')
    if analysis_sleep_time < _MIN_ANALYSIS_SLEEP_TIME:
        raise ValueError('To avoid making too many service requests please set '
                         f'analysis_sleep_time>={_MIN_ANALYSIS_SLEEP_TIME}')

    batch_client = SentinelHubBatch(config=config)

    batch_request = batch_client.get_request(batch_request)
    while batch_request.status in [BatchRequestStatus.CREATED,
                                   BatchRequestStatus.ANALYSING]:
        LOGGER.info('Batch job has a status %s, sleeping for %d seconds', batch_request.status.value,
                    analysis_sleep_time)
        time.sleep(analysis_sleep_time)

        batch_request = batch_client.get_request(batch_request)

    batch_request.raise_for_status(status=[BatchRequestStatus.FAILED, BatchRequestStatus.CANCELED])

    if batch_request.status is BatchRequestStatus.PROCESSING:
        LOGGER.info('Batch job is running')

    tiles_per_status = _get_batch_tiles_per_status(batch_request, batch_client)
    success_count = len(tiles_per_status[BatchTileStatus.PROCESSED])
    finished_count = success_count + len(tiles_per_status[BatchTileStatus.FAILED])

    with tqdm(total=batch_request.tile_count, initial=finished_count, desc='Progress rate') as progress_bar, \
            tqdm(total=finished_count, initial=success_count, desc='Success rate') as success_bar:
        while finished_count < batch_request.tile_count:
            time.sleep(sleep_time)

            tiles_per_status = _get_batch_tiles_per_status(batch_request, batch_client)
            new_success_count = len(tiles_per_status[BatchTileStatus.PROCESSED])
            new_finished_count = new_success_count + len(tiles_per_status[BatchTileStatus.FAILED])

            progress_bar.update(new_finished_count - finished_count)
            if new_finished_count != finished_count:
                success_bar.total = new_finished_count
                success_bar.refresh()
            success_bar.update(new_success_count - success_count)

            finished_count = new_finished_count
            success_count = new_success_count

    failed_tiles_num = finished_count - success_count
    if failed_tiles_num:
        LOGGER.info('Batch job failed for %d tiles', failed_tiles_num)
    return tiles_per_status


def _get_batch_tiles_per_status(batch_request, batch_client):
    """ A helper function that queries information about batch tiles and returns information about tiles, grouped by
    tile status.

    :return: A dictionary mapping a tile status to a list of tile payloads.
    :rtype: defaultdict(BatchTileStatus, list(dict))
    """
    tiles_per_status = defaultdict(list)

    for tile in batch_client.iter_tiles(batch_request):
        status = BatchTileStatus(tile['status'])
        tiles_per_status[status].append(tile)

    return tiles_per_status

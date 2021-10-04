"""
Main module for collecting data
"""

import datetime
import os
import logging
import copy
from abc import ABC, abstractmethod

from .config import SHConfig
from .ogc import OgcImageService
from .fis import FisService
from .geopedia import GeopediaWmsService, GeopediaImageService
from .aws import AwsProduct, AwsTile
from .aws_safe import SafeProduct, SafeTile
from .data_collections import handle_deprecated_data_source
from .download import DownloadRequest, DownloadClient, AwsDownloadClient, SentinelHubDownloadClient
from .os_utils import make_folder
from .constants import MimeType, CustomUrlParam, ServiceType, CRS, HistogramType
from .data_collections import DataCollection

LOGGER = logging.getLogger(__name__)


class DataRequest(ABC):
    """ Abstract class for all Sentinel Hub data requests.

    Every data request type can write the fetched data to disk and then read it again (and hence avoid the need to
    download the same data again).
    """
    def __init__(self, download_client_class, *, data_folder=None, config=None):
        """
        :param download_client_class: A class implementing a download client
        :type download_client_class: type
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.download_client_class = download_client_class
        self.data_folder = data_folder
        self.config = config or SHConfig()

        self.download_list = []
        self.folder_list = []
        self.create_request()

    @abstractmethod
    def create_request(self):
        """ An abstract method for logic of creating download requests
        """
        raise NotImplementedError

    def get_download_list(self):
        """
        Returns a list of download requests for requested data.

        :return: List of data to be downloaded
        :rtype: list(sentinelhub.DownloadRequest)
        """
        return self.download_list

    def get_filename_list(self):
        """ Returns a list of file names (or paths relative to `data_folder`) where the requested data will be saved
        or read from, if it has already been downloaded and saved.

        :return: A list of filenames
        :rtype: list(str)
        """
        return [request.get_relative_paths()[1] for request in self.download_list]

    def get_url_list(self):
        """
        Returns a list of urls for requested data.

        :return: List of URLs from where data will be downloaded.
        :rtype: list(str)
        """
        return [request.url for request in self.download_list]

    def is_valid_request(self):
        """ Checks if initialized class instance successfully prepared a list of items to download

        :return: `True` if request is valid and `False` otherwise
        :rtype: bool
        """
        return isinstance(self.download_list, list) and \
            all(isinstance(request, DownloadRequest) for request in self.download_list)

    def get_data(self, *, save_data=False, redownload=False, data_filter=None, max_threads=None,
                 decode_data=True, raise_download_errors=True):
        """ Get requested data either by downloading it or by reading it from the disk (if it
        was previously downloaded and saved).

        :param save_data: flag to turn on/off saving of data to disk. Default is `False`.
        :type save_data: bool
        :param redownload: if `True`, download again the requested data even though it's already saved to disk.
                            Default is `False`, do not download if data is already available on disk.
        :type redownload: bool
        :param data_filter: Used to specify which items will be returned by the method and in which order. E.g. with
            ``data_filter=[0, 2, -1]`` the method will return only 1st, 3rd and last item. Default filter is `None`.
        :type data_filter: list(int) or None
        :param max_threads: Maximum number of threads to be used for download in parallel. The default is
            `max_threads=None` which will use the number of processors on the system multiplied by 5.
        :type max_threads: int or None
        :param decode_data: If `True` (default) it decodes data (e.g., returns image as an array of numbers);
            if `False` it returns binary data.
        :type decode_data: bool
        :param raise_download_errors: If `True` any error in download process should be raised as
            ``DownloadFailedException``. If `False` failed downloads will only raise warnings and the method will
            return list with `None` values in places where the results of failed download requests should be.
        :type raise_download_errors: bool
        :return: requested images as numpy arrays, where each array corresponds to a single acquisition and has
                    shape ``[height, width, channels]``.
        :rtype: list of numpy arrays
        """
        self._preprocess_request(save_data, True)
        return self._execute_data_download(data_filter, redownload, max_threads, raise_download_errors,
                                           decode_data=decode_data)

    def save_data(self, *, data_filter=None, redownload=False, max_threads=None, raise_download_errors=False):
        """ Saves data to disk. If ``redownload=True`` then the data is redownloaded using ``max_threads`` workers.

        :param data_filter: Used to specify which items will be returned by the method and in which order. E.g. with
            `data_filter=[0, 2, -1]` the method will return only 1st, 3rd and last item. Default filter is `None`.
        :type data_filter: list(int) or None
        :param redownload: data is redownloaded if ``redownload=True``. Default is `False`
        :type redownload: bool
        :param max_threads: Maximum number of threads to be used for download in parallel. The default is
            `max_threads=None` which will use the number of processors on the system multiplied by 5.
        :type max_threads: int or None
        :param raise_download_errors: If `True` any error in download process should be raised as
            ``DownloadFailedException``. If `False` failed downloads will only raise warnings.
        :type raise_download_errors: bool
        """
        self._preprocess_request(True, False)
        self._execute_data_download(data_filter, redownload, max_threads, raise_download_errors)

    def _execute_data_download(self, data_filter, redownload, max_threads, raise_download_errors, decode_data=True):
        """ Calls download module and executes the download process

        :param data_filter: Used to specify which items will be returned by the method and in which order. E.g. with
            `data_filter=[0, 2, -1]` the method will return only 1st, 3rd and last item. Default filter is `None`.
        :type data_filter: list(int) or None
        :param redownload: data is redownloaded if ``redownload=True``. Default is `False`
        :type redownload: bool
        :param max_threads: Maximum number of threads to be used for download in parallel. The default is
            `max_threads=None` which will use the number of processors on the system multiplied by 5.
        :type max_threads: int or None
        :param raise_download_errors: If `True` any error in download process should be raised as
            ``DownloadFailedException``. If `False` failed downloads will only raise warnings.
        :type raise_download_errors: bool
        :param decode_data: If `True` (default) it decodes data (e.g., returns image as an array of numbers);
            if `False` it returns binary data.
        :type decode_data: bool
        :return: List of data obtained from download
        :rtype: list
        """
        is_repeating_filter = False
        if data_filter is None:
            filtered_download_list = self.download_list
        elif isinstance(data_filter, (list, tuple)):
            try:
                filtered_download_list = [self.download_list[index] for index in data_filter]
            except IndexError as exception:
                raise IndexError('Indices of data_filter are out of range') from exception

            filtered_download_list, mapping_list = self._filter_repeating_items(filtered_download_list)
            is_repeating_filter = len(filtered_download_list) < len(mapping_list)
        else:
            raise ValueError('data_filter parameter must be a list of indices')

        client = self.download_client_class(
            redownload=redownload,
            raise_download_errors=raise_download_errors,
            config=self.config
        )
        data_list = client.download(filtered_download_list, max_threads=max_threads, decode_data=decode_data)

        if is_repeating_filter:
            data_list = [copy.deepcopy(data_list[index]) for index in mapping_list]

        return data_list

    @staticmethod
    def _filter_repeating_items(download_list):
        """ Because of data_filter some requests in download list might be the same. In order not to download them again
        this method will reduce the list of requests. It will also return a mapping list which can be used to
        reconstruct the previous list of download requests.

        :param download_list: List of download requests
        :type download_list: list(sentinelhub.DownloadRequest)
        :return: reduced download list with unique requests and mapping list
        :rtype: (list(sentinelhub.DownloadRequest), list(int))
        """
        unique_requests_map = {}
        mapping_list = []
        unique_download_list = []
        for download_request in download_list:
            if download_request not in unique_requests_map:
                unique_requests_map[download_request] = len(unique_download_list)
                unique_download_list.append(download_request)
            mapping_list.append(unique_requests_map[download_request])
        return unique_download_list, mapping_list

    def _preprocess_request(self, save_data, return_data):
        """ Prepares requests for download and creates empty folders

        :param save_data: Tells whether to save data or not
        :type save_data: bool
        :param return_data: Tells whether to return data or not
        :type return_data: bool
        """
        if not self.is_valid_request():
            raise ValueError('Cannot obtain data because request is invalid')

        if save_data and self.data_folder is None:
            raise ValueError('Request parameter `data_folder` is not specified. '
                             'In order to save data please set `data_folder` to location on your disk.')

        for download_request in self.download_list:
            download_request.save_response = save_data
            download_request.return_data = return_data
            download_request.data_folder = self.data_folder

        if save_data:
            for folder in self.folder_list:
                make_folder(os.path.join(self.data_folder, folder))


class OgcRequest(DataRequest):
    """ The base class for OGC-type requests (WMS and WCS) where all common parameters are defined
    """
    def __init__(self, layer, bbox, *, time='latest', service_type=None, data_collection=None,
                 size_x=None, size_y=None, maxcc=1.0, image_format=MimeType.PNG, custom_url_params=None,
                 time_difference=datetime.timedelta(seconds=-1), data_source=None, **kwargs):
        """
        :param layer: An ID of a layer configured in Sentinel Hub Dashboard. It has to be configured for the same
            instance ID which will be used for this request. Also the satellite collection of the layer in Dashboard
            must match the one given by `data_collection` parameter
        :type layer: str
        :param bbox: Bounding box of the requested image. Coordinates must be in the specified coordinate reference
            system.
        :type bbox: geometry.BBox
        :param time: time or time range for which to return the results, in ISO8601 format
            (year-month-date, for example: ``2016-01-01``, or year-month-dateThours:minutes:seconds format,
            i.e. ``2016-01-01T16:31:21``). When a single time is specified the request will return data for that
            specific date, if it exists. If a time range is specified the result is a list of all scenes between the
            specified dates conforming to the cloud coverage criteria. Most recent acquisition being first in the list.
            For the latest acquisition use ``latest``. Examples: ``latest``, ``'2016-01-01'``, or
            ``('2016-01-01', ' 2016-01-31')``
        :type time: str or (str, str) or datetime.date or (datetime.date, datetime.date) or datetime.datetime or
            (datetime.datetime, datetime.datetime)
        :param service_type: type of OGC service (WMS or WCS)
        :type service_type: constants.ServiceType
        :param data_collection: A collection of requested satellite data. It has to be the same as defined in
            Sentinel Hub Dashboard for the given layer.
        :type data_collection: DataCollection
        :param size_x: number of pixels in x or resolution in x (i.e. ``512`` or ``10m``)
        :type size_x: int or str
        :param size_y: number of pixels in x or resolution in y (i.e. ``512`` or ``10m``)
        :type size_y: int or str
        :param maxcc: maximum accepted cloud coverage of an image. Float between 0.0 and 1.0. Default is ``1.0``.
        :type maxcc: float
        :param image_format: format of the returned image by the Sentinel Hub's WMS getMap service. Default is PNG, but
                            in some cases 32-bit TIFF is required, i.e. if requesting unprocessed raw bands.
                            Default is ``constants.MimeType.PNG``.
        :type image_format: constants.MimeType
        :param custom_url_params: A dictionary of CustomUrlParameters and their values supported by Sentinel Hub's WMS
            and WCS services. All available parameters are described at
            http://www.sentinel-hub.com/develop/documentation/api/custom-url-parameters. Note: in case of
            `CustomUrlParam.EVALSCRIPT` the dictionary value must be a string of Javascript code that is not
            encoded into base64.
        :type custom_url_params: Dict[CustomUrlParameter, object]
        :param time_difference: The time difference below which dates are deemed equal. That is, if for the given set
            of OGC parameters the images are available at datestimes `d1<=d2<=...<=dn` then only those with
            `dk-dj>time_difference` will be considered. The default time difference is negative (`-1s`), meaning
            that all dates are considered by default.
        :type time_difference: datetime.timedelta
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        :param data_source: A deprecated alternative to data_collection
        :type data_source: DataCollection
        """
        self.layer = layer
        self.bbox = bbox
        self.time = time
        self.data_collection = DataCollection(handle_deprecated_data_source(data_collection, data_source,
                                                                            default=DataCollection.SENTINEL2_L1C))
        self.maxcc = maxcc
        self.image_format = MimeType(image_format)
        self.service_type = service_type
        self.size_x = size_x
        self.size_y = size_y
        self.custom_url_params = custom_url_params
        self.time_difference = time_difference

        if self.custom_url_params is not None:
            self._check_custom_url_parameters()

        self.wfs_iterator = None

        super().__init__(SentinelHubDownloadClient, **kwargs)

    def _check_custom_url_parameters(self):
        """ Checks if custom url parameters are valid parameters.

        Throws ValueError if the provided parameter is not a valid parameter.
        """
        for param in self.custom_url_params:
            if param not in CustomUrlParam:
                raise ValueError(f'Parameter {param} is not a valid custom url parameter. Please check and fix.')

        if self.service_type is ServiceType.FIS and CustomUrlParam.GEOMETRY in self.custom_url_params:
            raise ValueError(f'{CustomUrlParam.GEOMETRY} should not be a custom url parameter of a FIS request')

    def create_request(self, reset_wfs_iterator=False):
        """ Set download requests

        Create a list of DownloadRequests for all Sentinel-2 acquisitions within request's time interval and
        acceptable cloud coverage.

        :param reset_wfs_iterator: When re-running the method this flag is used to reset/keep existing ``wfs_iterator``
            (i.e. instance of ``WebFeatureService`` class). If the iterator is not reset you don't have to repeat a
            service call but tiles and dates will stay the same.
        :type reset_wfs_iterator: bool
        """
        if reset_wfs_iterator:
            self.wfs_iterator = None

        ogc_service = OgcImageService(config=self.config)
        self.download_list = ogc_service.get_request(self)
        self.wfs_iterator = ogc_service.get_wfs_iterator()

    def get_dates(self):
        """ Get list of dates

        List of all available Sentinel-2 acquisitions for given bbox with max cloud coverage and the specified
        time interval. When a single time is specified the request will return that specific date, if it exists.
        If a time range is specified the result is a list of all scenes between the specified dates conforming to
        the cloud coverage criteria. Most recent acquisition being first in the list.

        :return: list of all available Sentinel-2 acquisition times within request's time interval and
                acceptable cloud coverage.
        :rtype: list(datetime.datetime) or [None]
        """
        return OgcImageService(config=self.config).get_dates(self)

    def get_tiles(self):
        """ Returns iterator over info about all satellite tiles used for the OgcRequest

        :return: Iterator of dictionaries containing info about all satellite tiles used in the request. In case of
                 DataCollection.DEM it returns None.
        :rtype: Iterator[dict] or None
        """
        return self.wfs_iterator


class WmsRequest(OgcRequest):
    """ Web Map Service request class

    Creates an instance of Sentinel Hub WMS (Web Map Service) GetMap request,
    which provides access to Sentinel-2's unprocessed bands (B01, B02, ..., B08, B8A, ..., B12)
    or processed products such as true color imagery, NDVI, etc. The only difference is that in
    the case of WMS request the user specifies the desired image size instead of its resolution.

    It is required to specify at least one of `width` and `height` parameters. If only one of them is specified the
    the other one will be calculated to best fit the bounding box ratio. If both of them are specified they will be used
    no matter the bounding box ratio.

    More info available at:
    https://www.sentinel-hub.com/develop/documentation/api/ogc_api/wms-parameters
    """
    def __init__(self, *, width=None, height=None, **kwargs):
        """
        :param width: width (number of columns) of the returned image (array)
        :type width: int or None
        :param height: height (number of rows) of the returned image (array)
        :type height: int or None
        :param layer: An ID of a layer configured in Sentinel Hub Dashboard. It has to be configured for the same
            instance ID which will be used for this request. Also the satellite collection of the layer in Dashboard
            must match the one given by `data_collection` parameter
        :type layer: str
        :param bbox: Bounding box of the requested image. Coordinates must be in the specified coordinate reference
            system.
        :type bbox: geometry.BBox
        :param time: time or time range for which to return the results, in ISO8601 format
            (year-month-date, for example: ``2016-01-01``, or year-month-dateThours:minutes:seconds format,
            i.e. ``2016-01-01T16:31:21``). When a single time is specified the request will return data for that
            specific date, if it exists. If a time range is specified the result is a list of all scenes between the
            specified dates conforming to the cloud coverage criteria. Most recent acquisition being first in the list.
            For the latest acquisition use ``latest``. Examples: ``latest``, ``'2016-01-01'``, or
            ``('2016-01-01', ' 2016-01-31')``
        :type time: str or (str, str) or datetime.date or (datetime.date, datetime.date) or datetime.datetime or
            (datetime.datetime, datetime.datetime)
        :param data_collection: A collection of requested satellite data. It has to be the same as defined in
            Sentinel Hub Dashboard for the given layer. Default is Sentinel-2 L1C.
        :type data_collection: DataCollection
        :param size_x: number of pixels in x or resolution in x (i.e. ``512`` or ``10m``)
        :type size_x: int or str
        :param size_y: number of pixels in x or resolution in y (i.e. ``512`` or ``10m``)
        :type size_y: int or str
        :param maxcc: maximum accepted cloud coverage of an image. Float between 0.0 and 1.0. Default is ``1.0``.
        :type maxcc: float
        :param image_format: format of the returned image by the Sentinel Hub's WMS getMap service. Default is PNG, but
                            in some cases 32-bit TIFF is required, i.e. if requesting unprocessed raw bands.
                            Default is ``constants.MimeType.PNG``.
        :type image_format: constants.MimeType
        :param custom_url_params: A dictionary of CustomUrlParameters and their values supported by Sentinel Hub's WMS
            and WCS services. All available parameters are described at
            http://www.sentinel-hub.com/develop/documentation/api/custom-url-parameters. Note: in case of
            `CustomUrlParam.EVALSCRIPT` the dictionary value must be a string of Javascript code that is not
            encoded into base64.
        :type custom_url_params: Dict[CustomUrlParameter, object]
        :param time_difference: The time difference below which dates are deemed equal. That is, if for the given set
            of OGC parameters the images are available at datestimes `d1<=d2<=...<=dn` then only those with
            `dk-dj>time_difference` will be considered. The default time difference is negative (`-1s`), meaning
            that all dates are considered by default.
        :type time_difference: datetime.timedelta
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        :param data_source: A deprecated alternative to data_collection
        :type data_source: DataCollection
        """
        super().__init__(service_type=ServiceType.WMS, size_x=width, size_y=height, **kwargs)


class WcsRequest(OgcRequest):
    """ Web Coverage Service request class

    Creates an instance of Sentinel Hub WCS (Web Coverage Service) GetCoverage request,
    which provides access to Sentinel-2's unprocessed bands (B01, B02, ..., B08, B8A, ..., B12)
    or processed products such as true color imagery, NDVI, etc., as the WMS service. The
    only difference is that in the case of WCS request the user specifies the desired
    resolution of the image instead of its size.

    More info available at:
    https://www.sentinel-hub.com/develop/documentation/api/ogc_api/wcs-request
    """
    def __init__(self, *, resx='10m', resy='10m', **kwargs):
        """
        :param resx: resolution in x (resolution of a column) given in meters in the format (examples ``10m``,
            ``20m``, ...). Default is ``10m``, which is the best native resolution of some Sentinel-2 bands.
        :type resx: str
        :param resy: resolution in y (resolution of a row) given in meters in the format
            (examples ``10m``, ``20m``, ...). Default is ``10m``, which is the best native resolution of some
            Sentinel-2 bands.
        :type resy: str
        :param layer: An ID of a layer configured in Sentinel Hub Dashboard. It has to be configured for the same
            instance ID which will be used for this request. Also the satellite collection of the layer in Dashboard
            must match the one given by `data_collection` parameter
        :type layer: str
        :param bbox: Bounding box of the requested image. Coordinates must be in the specified coordinate reference
            system.
        :type bbox: geometry.BBox
        :param time: time or time range for which to return the results, in ISO8601 format
            (year-month-date, for example: ``2016-01-01``, or year-month-dateThours:minutes:seconds format,
            i.e. ``2016-01-01T16:31:21``). When a single time is specified the request will return data for that
            specific date, if it exists. If a time range is specified the result is a list of all scenes between the
            specified dates conforming to the cloud coverage criteria. Most recent acquisition being first in the list.
            For the latest acquisition use ``latest``. Examples: ``latest``, ``'2016-01-01'``, or
            ``('2016-01-01', ' 2016-01-31')``
        :type time: str or (str, str) or datetime.date or (datetime.date, datetime.date) or datetime.datetime or
            (datetime.datetime, datetime.datetime)
        :param data_collection: A collection of requested satellite data. It has to be the same as defined in Sentinel
            Hub Dashboard for the given layer. Default is Sentinel-2 L1C.
        :type data_collection: DataCollection
        :param size_x: number of pixels in x or resolution in x (i.e. ``512`` or ``10m``)
        :type size_x: int or str
        :param size_y: number of pixels in x or resolution in y (i.e. ``512`` or ``10m``)
        :type size_y: int or str
        :param maxcc: maximum accepted cloud coverage of an image. Float between 0.0 and 1.0. Default is ``1.0``.
        :type maxcc: float
        :param image_format: format of the returned image by the Sentinel Hub's WMS getMap service. Default is PNG, but
                            in some cases 32-bit TIFF is required, i.e. if requesting unprocessed raw bands.
                            Default is ``constants.MimeType.PNG``.
        :type image_format: constants.MimeType
        :param custom_url_params: A dictionary of CustomUrlParameters and their values supported by Sentinel Hub's WMS
            and WCS services. All available parameters are described at
            http://www.sentinel-hub.com/develop/documentation/api/custom-url-parameters. Note: in case of
            `CustomUrlParam.EVALSCRIPT` the dictionary value must be a string of Javascript code that is not
            encoded into base64.
        :type custom_url_params: Dict[CustomUrlParameter, object]
        :param time_difference: The time difference below which dates are deemed equal. That is, if for the given set
            of OGC parameters the images are available at datestimes `d1<=d2<=...<=dn` then only those with
            `dk-dj>time_difference` will be considered. The default time difference is negative (`-1s`), meaning
            that all dates are considered by default.
        :type time_difference: datetime.timedelta
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        :param data_source: A deprecated alternative to data_collection
        :type data_source: DataCollection
        """
        super().__init__(service_type=ServiceType.WCS, size_x=resx, size_y=resy, **kwargs)


class FisRequest(OgcRequest):
    """ The Statistical info (or feature info service, abbreviated FIS) request class

    The Statistical info (or feature info service, abbreviated FIS), performs elementary statistical
    computations---such as mean, standard deviation, and histogram approximating the distribution of reflectance
    values---on remotely sensed data for a region specified in a given spatial reference system across different
    bands and time ranges.

    A quintessential usage example would be querying the service for basic statistics and the distribution of NDVI
    values for a polygon representing an agricultural unit over a time range.

    More info available at:
    https://www.sentinel-hub.com/develop/documentation/api/ogc_api/wcs-request
    """
    def __init__(self, layer, time, geometry_list, *, resolution='10m', bins=None, histogram_type=None, **kwargs):
        """
        :param layer: An ID of a layer configured in Sentinel Hub Dashboard. It has to be configured for the same
            instance ID which will be used for this request. Also the satellite collection of the layer in Dashboard
            must match the one given by `data_collection` parameter
        :type layer: str
        :param time: time or time range for which to return the results, in ISO8601 format
            (year-month-date, for example: ``2016-01-01``, or year-month-dateThours:minutes:seconds format,
            i.e. ``2016-01-01T16:31:21``). Examples: ``'2016-01-01'``, or ``('2016-01-01', ' 2016-01-31')``
        :type time: str or (str, str) or datetime.date or (datetime.date, datetime.date) or datetime.datetime or
            (datetime.datetime, datetime.datetime)
        :param geometry_list: A WKT representation of a geometry describing the region of interest.
            Note that WCS 1.1.1 standard is used here, so for EPSG:4326 coordinates should be in latitude/longitude
            order.
        :type geometry_list: list, [geometry.Geometry or geometry.Bbox]
        :param resolution: Specifies the spatial resolution, in meters per pixel, of the image from which the statistics
            are to be estimated. When using CRS=EPSG:4326 one has to add the "m" suffix to
            enforce resolution in meters per pixel (e.g. RESOLUTION=10m).
        :type resolution: str
        :param bins: The number of bins (a positive integer) in the histogram. If this parameter is absent no histogram
            is computed.
        :type bins: str
        :param histogram_type: type of histogram
        :type histogram_type: HistogramType
        :param data_collection: A collection of requested satellite data. It has to be the same as defined in Sentinel
            Hub Dashboard for the given layer. Default is Sentinel-2 L1C.
        :type data_collection: DataCollection
        :param maxcc: maximum accepted cloud coverage of an image. Float between 0.0 and 1.0. Default is ``1.0``.
        :type maxcc: float
        :param custom_url_params: Dictionary of CustomUrlParameters and their values supported by Sentinel Hub's WMS
            and WCS services. All available parameters are described at
            http://www.sentinel-hub.com/develop/documentation/api/custom-url-parameters. Note: in
            case of constants.CustomUrlParam.EVALSCRIPT the dictionary value must be a string
            of Javascript code that is not encoded into base64.
        :type custom_url_params: Dict[CustomUrlParameter, object]
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        :param data_source: A deprecated alternative to data_collection
        :type data_source: DataCollection
        """
        self.geometry_list = geometry_list
        self.resolution = resolution
        self.bins = bins
        self.histogram_type = HistogramType(histogram_type) if histogram_type else None

        super().__init__(bbox=None, layer=layer, time=time, service_type=ServiceType.FIS, **kwargs)

    def create_request(self):
        """ Set download requests

        Create a list of DownloadRequests for all Sentinel-2 acquisitions within request's time interval and
        acceptable cloud coverage.
        """
        fis_service = FisService(config=self.config)
        self.download_list = fis_service.get_request(self)

    def get_dates(self):
        """ This method is not supported for FIS request
        """
        raise NotImplementedError

    def get_tiles(self):
        """ This method is not supported for FIS request
        """
        raise NotImplementedError


class GeopediaRequest(DataRequest):
    """ The base class for Geopedia requests where all common parameters are defined.
    """
    def __init__(self, layer, service_type, *, bbox=None, theme=None, image_format=MimeType.PNG, **kwargs):
        """
        :param layer: Geopedia layer which contains requested data
        :type layer: str
        :param service_type: Type of the service, supported are ``ServiceType.WMS`` and ``ServiceType.IMAGE``
        :type service_type: constants.ServiceType
        :param bbox: Bounding box of the requested data
        :type bbox: geometry.BBox
        :param theme: Geopedia's theme endpoint string for which the layer is defined. Only required by WMS service.
        :type theme: str
        :param image_format: Format of the returned image by the Sentinel Hub's WMS getMap service. Default is
            ``constants.MimeType.PNG``.
        :type image_format: constants.MimeType
        :param data_folder: Location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.layer = layer
        self.service_type = service_type

        self.bbox = bbox
        if bbox.crs is not CRS.POP_WEB:
            raise ValueError('Geopedia Request at the moment supports only bounding boxes with coordinates in '
                             f'{CRS.POP_WEB}')

        self.theme = theme
        self.image_format = MimeType(image_format)

        super().__init__(DownloadClient, **kwargs)

    @abstractmethod
    def create_request(self):
        raise NotImplementedError


class GeopediaWmsRequest(GeopediaRequest):
    """ Web Map Service request class for Geopedia

    Creates an instance of Geopedia's WMS (Web Map Service) GetMap request, which provides access to WMS layers in
    Geopedia.
    """
    def __init__(self, layer, theme, bbox, *, width=None, height=None, **kwargs):
        """
        :param layer: Geopedia layer which contains requested data
        :type layer: str
        :param theme: Geopedia's theme endpoint string for which the layer is defined.
        :type theme: str
        :param bbox: Bounding box of the requested data
        :type bbox: geometry.BBox
        :param width: width (number of columns) of the returned image (array)
        :type width: int or None
        :param height: height (number of rows) of the returned image (array)
        :type height: int or None
        :param image_format: Format of the returned image by the Sentinel Hub's WMS getMap service. Default is
            ``constants.MimeType.PNG``.
        :type image_format: constants.MimeType
        :param data_folder: Location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.size_x = width
        self.size_y = height

        super().__init__(layer=layer, theme=theme, bbox=bbox, service_type=ServiceType.WMS, **kwargs)

    def create_request(self):
        """ Set download requests

        Create a list of DownloadRequests for all Sentinel-2 acquisitions within request's time interval and
        acceptable cloud coverage.
        """
        gpd_service = GeopediaWmsService(config=self.config)
        self.download_list = gpd_service.get_request(self)


class GeopediaImageRequest(GeopediaRequest):
    """ Request to access data in a Geopedia vector / raster layer.
    """
    def __init__(self, *, image_field_name, keep_image_names=True, gpd_session=None, **kwargs):
        """
        :param image_field_name: Name of the field in the data table which holds images
        :type image_field_name: str
        :param keep_image_names: If `True` images will be saved with the same names as in Geopedia otherwise Geopedia
            hashes will be used as names. If there are multiple images with the same names in the Geopedia layer this
            parameter should be set to `False` to prevent images being overwritten.
        :type keep_image_names: bool
        :param layer: Geopedia layer which contains requested data
        :type layer: str
        :param bbox: Bounding box of the requested data
        :type bbox: geometry.BBox
        :param image_format: Format of the returned image by the Sentinel Hub's WMS getMap service. Default is
            ``constants.MimeType.PNG``.
        :type image_format: constants.MimeType
        :param gpd_session: Optional parameter for specifying a custom Geopedia session, which can also contain login
            credentials. This can be used for accessing private Geopedia layers. By default it is set to `None` and a
            basic Geopedia session without credentials will be created.
        :type gpd_session: GeopediaSession or None
        :param data_folder: Location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.image_field_name = image_field_name
        self.keep_image_names = keep_image_names
        self.gpd_session = gpd_session

        self.gpd_iterator = None

        super().__init__(service_type=ServiceType.IMAGE, **kwargs)

    def create_request(self, reset_gpd_iterator=False):
        """ Set a list of download requests

        Set a list of DownloadRequests for all images that are under the
        given property of the Geopedia's Vector layer.

        :param reset_gpd_iterator: When re-running the method this flag is used to reset/keep existing ``gpd_iterator``
            (i.e. instance of ``GeopediaFeatureIterator`` class). If the iterator is not reset you don't have to
            repeat a service call but tiles and dates will stay the same.
        :type reset_gpd_iterator: bool
        """
        if reset_gpd_iterator:
            self.gpd_iterator = None

        gpd_service = GeopediaImageService(config=self.config)
        self.download_list = gpd_service.get_request(self)
        self.gpd_iterator = gpd_service.get_gpd_iterator()

    def get_items(self):
        """ Returns iterator over info about data used for this request

        :return: Iterator of dictionaries containing info about data used in
                 this request.
        :rtype: Iterator[dict] or None
        """
        return self.gpd_iterator


class AwsRequest(DataRequest):
    """ The base class for Amazon Web Service request classes. Common parameters are defined here.

    Collects and provides data from AWS.

    AWS database is available at:
    http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/
    """
    def __init__(self, *, bands=None, metafiles=None, safe_format=False, **kwargs):
        """
        :param bands: List of Sentinel-2 bands for request. If `None` all bands will be obtained
        :type bands: list(str) or None
        :param metafiles: list of additional metafiles available on AWS
                          (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``)
        :type metafiles: list(str)
        :param safe_format: flag that determines the structure of saved data. If `True` it will be saved in .SAFE format
                            defined by ESA. If `False` it will be saved in the same structure as the structure at AWS.
        :type safe_format: bool
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.bands = bands
        self.metafiles = metafiles
        self.safe_format = safe_format

        self.aws_service = None
        super().__init__(AwsDownloadClient, **kwargs)

    @abstractmethod
    def create_request(self):
        raise NotImplementedError

    def get_aws_service(self):
        """
        :return: initialized AWS service class
        :rtype: aws.AwsProduct or aws.AwsTile or aws_safe.SafeProduct or aws_safe.SafeTile
        """
        return self.aws_service


class AwsProductRequest(AwsRequest):
    """ AWS Service request class for an ESA product

    List of available products:
    http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/#products/
    """
    def __init__(self, product_id, *, tile_list=None, **kwargs):
        """
        :param product_id: original ESA product identification string
            (e.g. ``'S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551'``)
        :type product_id: str
        :param tile_list: list of tiles inside the product to be downloaded. If parameter is set to `None` all
            tiles inside the product will be downloaded.
        :type tile_list: list(str) or None
        :param bands: List of Sentinel-2 bands for request. If `None` all bands will be obtained
        :type bands: list(str) or None
        :param metafiles: list of additional metafiles available on AWS
            (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``)
        :type metafiles: list(str)
        :param safe_format: flag that determines the structure of saved data. If `True` it will be saved in .SAFE format
            defined by ESA. If `False` it will be saved in the same structure as the structure at AWS.
        :type safe_format: bool
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        """
        self.product_id = product_id
        self.tile_list = tile_list

        super().__init__(**kwargs)

    def create_request(self):
        if self.safe_format:
            self.aws_service = SafeProduct(self.product_id, tile_list=self.tile_list, bands=self.bands,
                                           metafiles=self.metafiles, config=self.config)
        else:
            self.aws_service = AwsProduct(self.product_id, tile_list=self.tile_list, bands=self.bands,
                                          metafiles=self.metafiles, config=self.config)

        self.download_list, self.folder_list = self.aws_service.get_requests()


class AwsTileRequest(AwsRequest):
    """ AWS Service request class for an ESA tile

    List of available products:
    http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/#tiles/
    """
    def __init__(self, *, tile=None, time=None, aws_index=None, data_collection=None, data_source=None, **kwargs):
        """
        :param tile: tile name (e.g. ``'T10UEV'``)
        :type tile: str
        :param time: tile sensing time in ISO8601 format
        :type time: str
        :param aws_index: there exist Sentinel-2 tiles with the same tile and time parameter. Therefore each tile on
            AWS also has an index which is visible in their url path. If aws_index is set to `None` the class
            will try to find the index automatically. If there will be multiple choices it will choose the
            lowest index and inform the user.
        :type aws_index: int or None
        :param data_collection: A collection of requested AWS data. Supported collections are Sentinel-2 L1C and
            Sentinel-2 L2A.
        :type data_collection: DataCollection
        :param bands: List of Sentinel-2 bands for request. If `None` all bands will be obtained
        :type bands: list(str) or None
        :param metafiles: list of additional metafiles available on AWS
            (e.g. ``['metadata', 'tileInfo', 'preview/B01', 'TCI']``)
        :type metafiles: list(str)
        :param safe_format: flag that determines the structure of saved data. If `True` it will be saved in .SAFE
            format defined by ESA. If `False` it will be saved in the same structure as the structure at AWS.
        :type safe_format: bool
        :param data_folder: location of the directory where the fetched data will be saved.
        :type data_folder: str
        :param config: A custom instance of config class to override parameters from the saved configuration.
        :type config: SHConfig or None
        :param data_source: A deprecated alternative to data_collection
        :type data_source: DataCollection
        """
        self.tile = tile
        self.time = time
        self.aws_index = aws_index
        self.data_collection = DataCollection(handle_deprecated_data_source(data_collection, data_source,
                                                                            default=DataCollection.SENTINEL2_L1C))

        super().__init__(**kwargs)

    def create_request(self):
        if self.safe_format:
            self.aws_service = SafeTile(self.tile, self.time, self.aws_index, bands=self.bands,
                                        metafiles=self.metafiles, data_collection=self.data_collection,
                                        config=self.config)
        else:
            self.aws_service = AwsTile(self.tile, self.time, self.aws_index, bands=self.bands,
                                       metafiles=self.metafiles, data_collection=self.data_collection,
                                       config=self.config)

        self.download_list, self.folder_list = self.aws_service.get_requests()


def get_safe_format(product_id=None, tile=None, entire_product=False, bands=None,
                    data_collection=None, data_source=None):
    """ Returns .SAFE format structure in form of nested dictionaries. Either ``product_id`` or ``tile`` must be
    specified.

    :param product_id: original ESA product identification string. Default is `None`
    :type product_id: str
    :param tile: tuple containing tile name and sensing time/date. Default is `None`
    :type tile: (str, str)
    :param entire_product: in case tile is specified this flag determines if it will be place inside a .SAFE structure
        of the product. Default is `False`
    :type entire_product: bool
    :param bands: list of bands to download. If `None` all bands will be downloaded. Default is `None`
    :type bands: list(str) or None
    :param data_collection: In case of tile request the collection of satellite data has to be specified.
    :type data_collection: DataCollection
    :param data_source: A deprecated alternative to data_collection
    :type data_source: DataCollection
    :return: Nested dictionaries representing .SAFE structure.
    :rtype: dict
    """
    data_collection = handle_deprecated_data_source(data_collection, data_source)

    entire_product = entire_product and product_id is None
    if tile is not None:
        safe_tile = SafeTile(tile_name=tile[0], time=tile[1], bands=bands, data_collection=data_collection)
        if not entire_product:
            return safe_tile.get_safe_struct()
        product_id = safe_tile.get_product_id()
    if product_id is None:
        raise ValueError('Either product_id or tile must be specified')
    safe_product = SafeProduct(product_id, tile_list=[tile[0]], bands=bands) if entire_product else \
        SafeProduct(product_id, bands=bands)
    return safe_product.get_safe_struct()


def download_safe_format(product_id=None, tile=None, folder='.', redownload=False, entire_product=False, bands=None,
                         data_collection=None, data_source=None):
    """ Downloads .SAFE format structure in form of nested dictionaries. Either ``product_id`` or ``tile`` must
    be specified.

    :param product_id: original ESA product identification string. Default is `None`
    :type product_id: str
    :param tile: tuple containing tile name and sensing time/date. Default is `None`
    :type tile: (str, str)
    :param folder: location of the directory where the fetched data will be saved. Default is ``'.'``
    :type folder: str
    :param redownload: if `True`, download again the requested data even though it's already saved to disk. If
        `False`, do not download if data is already available on disk. Default is `False`
    :type redownload: bool
    :param entire_product: in case tile is specified this flag determines if it will be place inside a .SAFE structure
        of the product. Default is `False`
    :type entire_product: bool
    :param bands: list of bands to download. If `None` all bands will be downloaded. Default is `None`
    :type bands: list(str) or None
    :param data_collection: In case of tile request the collection of satellite data has to be specified.
    :type data_collection: DataCollection
    :param data_source: A deprecated alternative to data_collection
    :type data_source: DataCollection
    :return: Nested dictionaries representing .SAFE structure.
    :rtype: dict
    """
    data_collection = handle_deprecated_data_source(data_collection, data_source)

    entire_product = entire_product and product_id is None
    if tile is not None:
        safe_request = AwsTileRequest(tile=tile[0], time=tile[1], data_folder=folder, bands=bands,
                                      safe_format=True, data_collection=data_collection)
        if entire_product:
            safe_tile = safe_request.get_aws_service()
            product_id = safe_tile.get_product_id()
    if product_id is not None:
        safe_request = AwsProductRequest(product_id, tile_list=[tile[0]], data_folder=folder, bands=bands,
                                         safe_format=True) if entire_product else \
            AwsProductRequest(product_id, data_folder=folder, bands=bands, safe_format=True)

    safe_request.save_data(redownload=redownload)

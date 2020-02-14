""" SentinelHubRequest for the Processing API

Documentation: https://docs.sentinel-hub.com/api/latest/reference/
"""

from datetime import datetime

from .config import SHConfig
from .constants import MimeType, DataSource, RequestType
from .download import DownloadRequest, SentinelHubDownloadClient
from .data_request import DataRequest
from .geometry import Geometry
from .geo_utils import bbox_to_dimensions


class SentinelHubRequest(DataRequest):
    """ Sentinel Hub API request class
    """
    def __init__(self, evalscript, input_data, responses, bbox=None, size=None, resolution=None, maxcc=1.0,
                 mosaicking_order='mostRecent', config=None, **kwargs):

        if size is None and resolution is None:
            raise ValueError("Either size or resolution argument should be given")

        size_x, size_y = size if size else bbox_to_dimensions(bbox, resolution)

        if not isinstance(evalscript, str):
            raise ValueError("'evalscript' should be a string")

        self.config = config or SHConfig()

        self.payload = self.body(
            request_bounds=self.bounds(crs=bbox.crs.opengis_string, bbox=list(bbox)),
            request_data=input_data,
            request_output=self.output(size_x=size_x, size_y=size_y, responses=responses),
            evalscript=evalscript
        )

        super().__init__(SentinelHubDownloadClient, **kwargs)

    def get_data(self, save_data=False, redownload=False, max_threads=None):
        """ Overrides DataRequest.get_data
        """
        self.download_list = [self._download_request]
        return super().get_data(save_data=save_data, redownload=redownload, max_threads=max_threads)[0]

    def create_request(self):
        """ Prepares a download request
        """
        self._download_request = DownloadRequest(
            request_type=RequestType.POST,
            url=self.config.get_sh_processing_api_url(),
            post_values=self.payload,
            data_folder=self.data_folder,
            hash_save=bool(self.data_folder),
            # data_type=MimeType.TAR,
            data_type=MimeType.TIFF,
            # headers={"accept": "application/tar", 'content-type': 'application/json'}
            headers={'content-type': 'application/json'}
        )

    @staticmethod
    def input_data(data_source=None, time_interval=None, maxcc=1.0, mosaicking_order='mostRecent'):
        """ Generate request data
        """

        if not isinstance(data_source, DataSource):
            raise ValueError("'data_source' should be an instance of sentinelhub.DataSource")

        if not isinstance(maxcc, float) and (maxcc < 0 or maxcc > 1):
            raise ValueError('maxcc should be a float on an interval [0, 1]')

        if time_interval and not isinstance(time_interval, tuple):
            raise ValueError("'time_interval should be a tupe of of (datetime, datetime) or (str, str)")

        if time_interval and all(isinstance(time, str) for time in time_interval):
            time_from, time_to = time_interval
            if time_from > time_to:
                raise ValueError("'time_from' should not b greater than 'time_to'")
            date_from, date_to = datetime.fromisoformat(time_from), datetime.fromisoformat(time_to)
            time_interval = date_from.isoformat() + 'Z', date_to.isoformat() + 'Z'

        time_from, time_to = time_interval if time_interval else (None, None)

        mosaic_order_params = ["mostRecent", "leastRecent", "leastCC"]
        if mosaicking_order not in mosaic_order_params:
            msg = "{} is not a valid mosaickingOrder parameter, it should be one of: {}"
            raise ValueError(msg.format(mosaicking_order, mosaic_order_params))

        data_type = 'CUSTOM' if data_source.is_custom() else data_source.api_identifier()

        input_data_object = {
            "type": data_type,
            "dataFilter": {
                "timeRange": {
                    "from": "" if time_from is None else time_from,
                    "to": "" if time_to is None else time_to
                },
                "maxCloudCoverage": int(maxcc * 100),
                "mosaickingOrder": mosaicking_order,
            }
        }

        if data_type == 'CUSTOM':
            input_data_object['dataFilter']['collectionId'] = data_source.value

        return input_data_object

    @staticmethod
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

    @staticmethod
    def output_response(identifier, response_format):
        """ Generate request response
        """
        return {
            "identifier": identifier,
            "format": {
                'type': response_format
            }
        }

    @staticmethod
    def output(responses, size_x, size_y):
        """ Generate request output
        """
        return {
            "width": size_x,
            "height": size_y,
            "responses": responses
        }

    @staticmethod
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
            msg = 'Bounding box and geometry should have the same CRS, but {} and {} found'.format(bbox, geometry.crs)
            raise ValueError(msg)

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

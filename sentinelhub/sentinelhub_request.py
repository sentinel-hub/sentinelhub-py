""" SentinelHubRequest for the Processing API

Documentation: https://docs.sentinel-hub.com/api/latest/reference/
"""

from datetime import datetime

from .config import SHConfig
from .constants import MimeType, DataSource, RequestType
from .download import DownloadRequest, SentinelHubDownloadClient
from .data_request import DataRequest
from .geometry import Geometry, BBox


class SentinelHubRequest(DataRequest):
    """ Sentinel Hub API request class
    """
    def __init__(self, evalscript, input_data, responses, bounds=None, size=None, resolution=None, maxcc=1.0,
                 mosaicking_order='mostRecent', config=None, mime_type=MimeType.TIFF, **kwargs):
        """
        :param bounds: Bounding box or geometry object
        :type bounds: sentinelhub.BBox or sentinelhub.Geometry
        """

        if size is None and resolution is None:
            raise ValueError("Either size or resolution argument should be given")

        if not isinstance(evalscript, str):
            raise ValueError("'evalscript' should be a string")

        self.config = config or SHConfig()

        self.headers = {'content-type': 'application/json'} if len(responses) <= 1 else \
                       {'content-type': 'application/json', "accept": "application/tar"}

        self.mime_type = mime_type

        self.payload = self.body(
            request_bounds=self.bounds(bounds),
            request_data=input_data,
            request_output=self.output(size=size, resolution=resolution, responses=responses),
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
            data_type=self.mime_type,
            headers=self.headers
        )

    @staticmethod
    def input_data(data_source=None, time_interval=None, maxcc=1.0, mosaicking_order='mostRecent', other_args=None):
        """ Generate request data
        """

        if not isinstance(data_source, DataSource):
            raise ValueError("'data_source' should be an instance of sentinelhub.DataSource")

        if not isinstance(maxcc, float) and (maxcc < 0 or maxcc > 1):
            raise ValueError('maxcc should be a float on an interval [0, 1]')

        if time_interval and not isinstance(time_interval, tuple):
            raise ValueError("'time_interval should be a tupe of of (datetime, datetime) or (str, str)")

        if time_interval and all(isinstance(time, str) for time in time_interval):
            time_interval = datetime.fromisoformat(time_interval[0]), datetime.fromisoformat(time_interval[1])
        elif time_interval and not all(isinstance(time, datetime) for time in time_interval):
            raise ValueError("'time_interval should be a tupe of of (datetime, datetime) or (str, str)")

        if time_interval:
            date_from, date_to = time_interval
            if date_from > date_to:
                raise ValueError("'time_from' should not b greater than 'time_to'")
            date_from, date_to = date_from.isoformat() + 'Z', date_to.isoformat() + 'Z'
        else:
            date_from, date_to = "", ""

        mosaic_order_params = ["mostRecent", "leastRecent", "leastCC"]
        if mosaicking_order not in mosaic_order_params:
            msg = "{} is not a valid mosaickingOrder parameter, it should be one of: {}"
            raise ValueError(msg.format(mosaicking_order, mosaic_order_params))

        data_type = 'CUSTOM' if data_source.is_custom() else data_source.api_identifier()

        input_data_object = {
            "type": data_type,
            "dataFilter": {
                "timeRange": {"from": date_from, "to": date_to},
                "maxCloudCoverage": int(maxcc * 100),
                "mosaickingOrder": mosaicking_order,
            }
        }

        if data_type == 'CUSTOM':
            input_data_object['dataFilter']['collectionId'] = data_source.value

        if other_args:
            input_data_object.update(other_args)

        return input_data_object

    @staticmethod
    def body(request_bounds, request_data, evalscript, request_output=None, other_args=None):
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

        if other_args:
            request_body.update(other_args)

        return request_body

    @staticmethod
    def output_response(identifier, response_format, other_args=None):
        """ Generate request response
        """
        output_response = {
            "identifier": identifier,
            "format": {
                'type': response_format
            }
        }

        if other_args:
            output_response.update(other_args)

        return output_response

    @staticmethod
    def output(responses, size=None, resolution=None, other_args=None):
        """ Generate request output
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
            request_output.update(other_args)

        return request_output

    @staticmethod
    def bounds(bounds_obj, other_args=None):
        """ Generate request bounds
        """
        if isinstance(bounds_obj, BBox):
            bbox, geometry = bounds_obj, None
        elif isinstance(bounds_obj, Geometry):
            bbox, geometry = None, bounds_obj
        else:
            raise ValueError('Unsupported bounds object: {}'.format(bounds_obj))

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
            request_bounds.update(other_args)

        return request_bounds

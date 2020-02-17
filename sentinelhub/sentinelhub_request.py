""" SentinelHubRequest for the Processing API

Documentation: https://docs.sentinel-hub.com/api/latest/reference/
"""

from .config import SHConfig
from .constants import MimeType, DataSource, RequestType
from .download import DownloadRequest, SentinelHubDownloadClient
from .data_request import DataRequest
from .geometry import Geometry, BBox
from .time_utils import parse_time_interval


class SentinelHubRequest(DataRequest):
    """ Sentinel Hub API request class
    """
    def __init__(self, evalscript, input_data, responses, bbox=None, geometry=None, size=None, resolution=None,
                 config=None, mime_type=MimeType.TIFF, **kwargs):
        """
        :param bounds: Bounding box or geometry object
        :type bounds: sentinelhub.BBox or sentinelhub.Geometry
        """

        if size is None and resolution is None:
            raise ValueError("Either size or resolution argument should be given")

        if not isinstance(evalscript, str):
            raise ValueError("'evalscript' should be a string")

        self.config = config or SHConfig()

        self.mime_type = MimeType(mime_type)

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
        headers = {'content-type': MimeType.JSON.get_string(), "accept": self.mime_type.get_string()}

        self.download_list = [DownloadRequest(
            request_type=RequestType.POST,
            url=self.config.get_sh_processing_api_url(),
            post_values=self.payload,
            data_folder=self.data_folder,
            save_response=bool(self.data_folder),
            data_type=self.mime_type,
            headers=headers
        )]

    @staticmethod
    def input_data(data_source=None, time_interval=None, maxcc=1.0, mosaicking_order='mostRecent', other_args=None):
        """ Generate request data
        """

        if not isinstance(data_source, DataSource):
            raise ValueError("'data_source' should be an instance of sentinelhub.DataSource")

        if not isinstance(maxcc, float) and (maxcc < 0 or maxcc > 1):
            raise ValueError('maxcc should be a float on an interval [0, 1]')

        if time_interval:
            date_from, date_to = parse_time_interval(time_interval)
            date_from, date_to = date_from + 'Z', date_to + 'Z'
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
    def bounds(bbox=None, geometry=None, other_args=None):
        """ Generate request bounds
        """
        if bbox is None and geometry is None:
            raise ValueError("'bbox' and/or 'geometry' have to be provided.")

        if bbox and not isinstance(bbox, BBox):
            raise ValueError("'bbox' should be an instance of sentinelhub.BBox")

        if geometry and not isinstance(geometry, Geometry):
            raise ValueError("'geometry' should be an instance of sentinelhub.Geometry")

        if bbox and geometry and bbox.crs != geometry.crs:
            raise ValueError("bbox and geometry should be in the same CRS")

        if bbox is None:
            bbox = geometry.bbox

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

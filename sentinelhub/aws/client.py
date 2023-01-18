"""
Module implementing a download client that is adjusted to download from AWS
"""
import logging
import warnings
from typing import Any, Dict, Optional

try:
    from boto3 import Session
    from botocore.exceptions import NoCredentialsError
except ImportError as import_exception:
    raise ImportError(
        "To use AWS functionalities of this package you have to install sentinelhub[AWS] package extension"
    ) from import_exception

from ..config import SHConfig
from ..download.client import DownloadClient
from ..download.handlers import fail_missing_file
from ..download.models import DownloadRequest, DownloadResponse
from ..exceptions import AwsDownloadFailedException, deprecated_class

LOGGER = logging.getLogger(__name__)


@deprecated_class(message_suffix="It will remain in the codebase for now, but won't be actively maintained.")
class AwsDownloadClient(DownloadClient):
    """An AWS download client class"""

    GLOBAL_S3_CLIENTS: Dict[str, Any] = {}

    def __init__(self, *args: Any, boto_params: Optional[Dict[str, Any]] = None, **kwargs: Any):
        """
        :param args: Positional arguments propagated to `DownloadClient` class.
        :param boto_params: A dictionary of extra parameters that will be propagated to `botocore.client.S3.get_object`
            method. E.g. `{"RequestPayer": "requester"}`.
        :param kwargs: Keyword arguments propagated to `DownloadClient` class.
        """
        super().__init__(*args, **kwargs)

        self.boto_params = boto_params or {}

    @fail_missing_file
    def _execute_download(self, request: DownloadRequest) -> DownloadResponse:
        """Executes a download procedure"""
        if not self.is_s3_request(request):
            return super()._execute_download(request)

        s3_client = self.get_s3_client(self.config)

        response_content = self._do_download(request, s3_client)

        LOGGER.debug("Successful download from %s", request.url)
        return DownloadResponse(request=request, content=response_content)

    @classmethod
    def get_s3_client(cls, config: SHConfig) -> Any:
        """Provides a s3 client object"""
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")
        try:
            s3_client = Session().client(
                "s3",
                aws_access_key_id=config.aws_access_key_id or None,
                aws_secret_access_key=config.aws_secret_access_key or None,
                aws_session_token=config.aws_session_token or None,
            )
            cls.GLOBAL_S3_CLIENTS[config.aws_access_key_id] = s3_client

        except KeyError as exception:  # Sometimes creation of client fails, and we use the global client if it exists
            global_client = cls.GLOBAL_S3_CLIENTS.get(config.aws_access_key_id)
            if global_client is None:
                raise ValueError("Failed to create a client for download from AWS") from exception
            s3_client = global_client

        return s3_client

    def _do_download(self, request: DownloadRequest, s3_client: Any) -> bytes:
        """Does the download from s3"""
        if request.url is None:
            raise ValueError(f"Faulty request {request}, no URL specified.")
        _, _, bucket_name, url_key = request.url.split("/", 3)

        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=url_key, **self.boto_params)

            return response["Body"].read()
        except NoCredentialsError as exception:
            raise ValueError(
                "The requested data is in Requester Pays AWS bucket. In order to download the data please set "
                "your access key either in AWS credentials file or in sentinelhub config.json file using "
                "command line:\n"
                "$ sentinelhub.config --aws_access_key_id <your AWS key> --aws_secret_access_key "
                "<your AWS secret key>"
            ) from exception
        except s3_client.exceptions.NoSuchKey as exception:
            raise AwsDownloadFailedException(f"File in location {request.url} is missing") from exception
        except s3_client.exceptions.NoSuchBucket as exception:
            raise ValueError(f"Aws bucket {bucket_name} does not exist") from exception

    @staticmethod
    def is_s3_request(request: DownloadRequest) -> bool:
        """Checks if data has to be downloaded from AWS s3 bucket

        :return: `True` if url describes location at AWS s3 bucket and `False` otherwise
        """
        return request.url is not None and request.url.startswith("s3://")

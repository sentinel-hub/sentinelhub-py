"""
Module implementing download client that is adjusted to download from AWS
"""
import logging
import warnings

import boto3
from botocore.exceptions import NoCredentialsError

from ..exceptions import AwsDownloadFailedException
from .client import DownloadClient, get_json, get_xml
from .handlers import fail_missing_file


LOGGER = logging.getLogger(__name__)


class AwsDownloadClient(DownloadClient):
    """ An AWS download client class
    """

    GLOBAL_S3_CLIENT = None

    @fail_missing_file
    def _execute_download(self, request):
        """ Executes a download procedure
        """
        if not self.is_s3_request(request):
            return super()._execute_download(request)

        s3_client = self._get_s3_client()

        response_content = self._do_download(request, s3_client)

        LOGGER.debug('Successful download from %s', request.url)
        return response_content

    def _get_s3_client(self):
        """ Provides a s3 client object
        """
        key_args = {}
        if self.config.aws_access_key_id and self.config.aws_secret_access_key:
            key_args = {
                'aws_access_key_id': self.config.aws_access_key_id,
                'aws_secret_access_key': self.config.aws_secret_access_key
            }

        warnings.filterwarnings('ignore', category=ResourceWarning, message='unclosed.*<ssl.SSLSocket.*>')
        try:
            s3_client = boto3.Session().client('s3', **key_args)
            AwsDownloadClient.GLOBAL_S3_CLIENT = s3_client
        except KeyError:  # Sometimes creation of client fails and we use the global client if it exists
            if AwsDownloadClient.GLOBAL_S3_CLIENT is None:
                raise ValueError('Failed to create a client for download from AWS')
            s3_client = AwsDownloadClient.GLOBAL_S3_CLIENT

        return s3_client

    @staticmethod
    def _do_download(request, s3_client):
        """ Does the download from s3
        """
        _, _, bucket_name, url_key = request.url.split('/', 3)

        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=url_key, RequestPayer='requester')
            return response['Body'].read()
        except NoCredentialsError:
            raise ValueError(
                'The requested data is in Requester Pays AWS bucket. In order to download the data please set '
                'your access key either in AWS credentials file or in sentinelhub config.json file using '
                'command line:\n'
                '$ sentinelhub.config --aws_access_key_id <your AWS key> --aws_secret_access_key '
                '<your AWS secret key>')
        except s3_client.exceptions.NoSuchKey:
            raise AwsDownloadFailedException('File in location %s is missing' % request.url)
        except s3_client.exceptions.NoSuchBucket:
            raise ValueError('Aws bucket %s does not exist' % bucket_name)

    @staticmethod
    def is_s3_request(request):
        """ Checks if data has to be downloaded from AWS s3 bucket

        :return: `True` if url describes location at AWS s3 bucket and `False` otherwise
        :rtype: bool
        """
        return request.url.startswith('s3://')


def get_aws_json(*args, **kwargs):
    """ Download a json from AWS
    """
    return get_json(*args, download_client_class=AwsDownloadClient, **kwargs)


def get_aws_xml(*args, **kwargs):
    """ Download an xml from AWS
    """
    return get_xml(*args, download_client_class=AwsDownloadClient, **kwargs)

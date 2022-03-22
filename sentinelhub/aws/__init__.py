"""
The part of the package that implements AWS functionalities
"""

from .aws import AwsProduct, AwsTile
from .aws_safe import SafeProduct, SafeTile
from .client import AwsDownloadClient
from .constants import AwsConstants
from .data_request import AwsProductRequest, AwsTileRequest, download_safe_format, get_safe_format

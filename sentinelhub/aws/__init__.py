"""
The part of the package for interacting with Amazon Web Services (AWS)
"""

from .client import AwsDownloadClient
from .constants import AwsConstants
from .data import AwsProduct, AwsTile
from .data_safe import SafeProduct, SafeTile
from .request import AwsProductRequest, AwsTileRequest, download_safe_format, get_safe_format

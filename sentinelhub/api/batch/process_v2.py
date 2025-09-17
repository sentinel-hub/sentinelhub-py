"""
Deprecated module for Batch Processing v2 API. The contents have been moved to `sentinelhub.api.batch.process`.
"""

import warnings

from sentinelhub.api.batch.process import *  # noqa: F403 # pylint: disable=unused-wildcard-import, wildcard-import
from sentinelhub.exceptions import SHDeprecationWarning

warnings.warn(
    "The module sentinelhub.api.batch.process_v2 has been renamed, please use sentinelhub.api.batch.process instead.",
    SHDeprecationWarning,
    stacklevel=2,
)

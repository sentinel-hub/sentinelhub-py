"""
The part of the package for interacting with Geopedia API. Note that this part might in the future be moved into a
separate package!
"""

from .core import GeopediaFeatureIterator, GeopediaSession
from .request import GeopediaImageRequest, GeopediaWmsRequest

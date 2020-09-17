"""
Module defining custom package exceptions
"""

import warnings


class BaseSentinelHubException(Exception):
    """ A base package exception
    """


class DownloadFailedException(BaseSentinelHubException):
    """ General exception which is raised whenever download fails
    """


class OutOfRequestsException(DownloadFailedException):
    """ An exception raised whenever download cannot be done because user has run out of requests or processing units
    """


class AwsDownloadFailedException(DownloadFailedException):
    """ This exception is raised when download fails because of a missing file in AWS
    """


class ImageDecodingError(BaseSentinelHubException):
    """ This exception is raised when downloaded image is not properly decoded
    """


class SHDeprecationWarning(DeprecationWarning):
    """ A custom deprecation warning for sentinelhub-py package
    """


class SHUserWarning(UserWarning):
    """ A custom user warning for sentinelhub-py package
    """


class SHRuntimeWarning(RuntimeWarning):
    """ A custom runtime warning for sentinelhub-py package, it should be
    """


def handle_deprecated_data_source(data_collection, data_source, default=None):
    """ Joins parameters used to specify a data collection. In case data_source is given it raises a warning. In case
    both are given it raises an error. In case neither are given but there is a default collection it raises another
    warning.

    Note that this function is only temporary and will be removed in future package versions
    """
    if data_source is not None:
        warnings.warn('Parameter data_source is deprecated, use data_collection instead',
                      category=SHDeprecationWarning)

    if data_collection is not None and data_source is not None:
        raise ValueError('Only one of the parameters data_collection and data_source should be given')

    if data_collection is None and data_source is None and default is not None:
        warnings.warn('In the future please specify data_collection parameter, for now taking '
                      'DataCollection.SENTINEL2_L1C', category=SHDeprecationWarning)
        return default

    return data_collection or data_source


warnings.simplefilter('default', SHDeprecationWarning)
warnings.simplefilter('default', SHUserWarning)
warnings.simplefilter('always', SHRuntimeWarning)

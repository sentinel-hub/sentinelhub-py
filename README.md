[![Join the chat at https://gitter.im/sinergise/Lobby](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/sinergise/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/sentinel-hub/sentinelhub-py.svg?branch=master)](https://travis-ci.org/sentinel-hub/sentinelhub-py)

# Description

The **sentinelhub** Python package allows users to make OGC (WMS and WCS)
web requests to download and process satellite images within your Python
scripts. It supports Sentinel-2 L1C and L2A, Sentinel-1, Landsat 8, MODIS and DEM data source.

Version 1.0 is backwards compatible with previous releases,
and therefore allows users to also download raw data from AWS to .SAFE
format.

# Installation

The package requires a Python version >= 3.5. The package is available on
the PyPI package manager and can be installed with

```
$ pip install sentinelhub
```

To install the package manually, clone the repository and
```
$ python setup.py build
$ python setup.py install
```

The package is backward compatible with the previous version.

# Content

### OGC web service

Some of the major features introduced in version 1.0 are linked to one's [Sentinel Hub account](https://services.sentinel-hub.com/oauth/subscription):
 * support for Web Map Service (WMS) and Web Coverage Service (WCS) requests using your Sentinel Hub account;
 * support for standard and custom multi-spectra layers, such as unprocessed
 bands, true color imagery, or NDVI;
 * support for multi-temporal requests;
 * support for cloud coverage filtering;
 * support for different Coordinate Reference Systems;
 * support to read and write downloaded data to disk in the most common
 image and data formats.
 * support for various data sources (new in version 1.1.0):
   * Sentinel-2 L1C,
   * Sentinel-2 L2A,
   * Sentinel-1,
   * Landsat 8,
   * MODIS,
   * DEM


### AWS data download

The package allows to download Sentinel-2 data from Sentinel-2 on AWS
and reconstruct data into ESA .SAFE format. Sentinel Hub account is not required to use this functionality.

The following are implemented:
 * support of old and new (i.e. compact) .SAFE format;
 * support for downloading of either entire product, or a map of the .SAFE
 structure only;
 * support of command lines entries;
 * adjustable threaded downloads, and optional redownloads of existing data (not default);
 * requires either S-2 product ID, or tile name and date of a product.

### Documentation

For more information on the package and to access the documentation, visit [readthedocs](http://sentinelhub-py.readthedocs.io/).


# Examples

Jupyter notebooks on how to use the modules to execute OGC requests, or
download raw data from AWS in .SAFE format can be found in the [examples](examples/)
folder, or viewed in the [docs](http://sentinelhub-py.readthedocs.io/):
 * AWS data download cli ([link](http://sentinelhub-py.readthedocs.io/en/latest/aws_cli.html));
 * AWS data download using Jupyter notebook ([link](http://sentinelhub-py.readthedocs.io/en/latest/examples/aws_request.html));
 * Using OGC web services within Jupyter notebook ([link](http://sentinelhub-py.readthedocs.io/en/latest/examples/ogc_request.html)).

# License

See [LICENSE](LICENSE.md).

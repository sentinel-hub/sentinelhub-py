[![Package version](https://badge.fury.io/py/sentinelhub.svg)](https://pypi.org/project/sentinelhub/)
[![Conda version](https://img.shields.io/conda/vn/conda-forge/sentinelhub.svg)](https://anaconda.org/conda-forge/sentinelhub)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/sentinelhub.svg?style=flat-square)](https://pypi.org/project/sentinelhub/)
[![Build Status](https://github.com/sentinel-hub/sentinelhub-py/actions/workflows/ci_action.yml/badge.svg?branch=master)](https://github.com/sentinel-hub/sentinelhub-py/actions)
[![Docs status](https://readthedocs.org/projects/sentinelhub-py/badge/?version=latest)](http://sentinelhub-py.readthedocs.io/en/latest/)
[![Overall downloads](https://pepy.tech/badge/sentinelhub)](https://pepy.tech/project/sentinelhub)
[![Last month downloads](https://pepy.tech/badge/sentinelhub/month)](https://pepy.tech/project/sentinelhub)
[![](https://img.shields.io/pypi/l/sentinelhub.svg)](https://github.com/sentinel-hub/sentinelhub-py/blob/master/LICENSE.md)
[![Code coverage](https://codecov.io/gh/sentinel-hub/sentinelhub-py/branch/master/graph/badge.svg)](https://codecov.io/gh/sentinel-hub/sentinelhub-py)

## Introduction

The **sentinelhub** Python package is the official Python interface for [Sentinel Hub services](https://www.sentinel-hub.com/). It supports most of the services described in the [Sentinel Hub documentation](https://docs.sentinel-hub.com/api/latest/) and any type of [satellite data collections](https://docs.sentinel-hub.com/api/latest/data/), including Sentinel, Landsat, MODIS, DEM, and custom collections produced by users.

The package also provides a collection of basic tools and utilities for working with geospatial and satellite data. It builds on top of well known packages such as `numpy`, `shapely`, `pyproj`, etc. It is also a core dependency of [`eo-learn`](https://github.com/sentinel-hub/eo-learn) Python package for creating geospatial data-processing workflows.

The main package resources are [GitHub repository](https://github.com/sentinel-hub/sentinelhub-py), [documentation page](https://sentinelhub-py.readthedocs.io/en/latest/), and [Sentinel Hub forum](https://forum.sentinel-hub.com/).


## Installation

The package requires a Python version >= 3.6 and an installed C/C++ compiler. The package is available at
the PyPI package index and can be installed with

```
$ pip install sentinelhub --upgrade
```

Alternatively, the package can be installed with Conda from `conda-forge` channel

```
$ conda install -c conda-forge sentinelhub 
```

To install the package manually, clone the repository and run

```
$ pip install .
```

Before installing `sentinelhub` on **Windows** it is recommended to install `shapely` package from
[Unofficial Windows wheels repository](https://www.lfd.uci.edu/~gohlke/pythonlibs/)

Once installed the package can be configured according to [configuration instructions](http://sentinelhub-py.readthedocs.io/en/latest/configure.html) in documentation.


## Content

A high-level overview of the main functionalities:

- Sentinel Hub services
  * [Process API](https://docs.sentinel-hub.com/api/latest/api/process/),
  * [Catalog API](https://docs.sentinel-hub.com/api/latest/api/catalog/),
  * [Batch Processing API](https://docs.sentinel-hub.com/api/latest/api/batch/),
  * [BYOC API](https://docs.sentinel-hub.com/api/latest/api/byoc/),
  * [Statistical API](https://docs.sentinel-hub.com/api/latest/api/statistical/),
  * [OGC services (WMS/WCS/WFS)](https://docs.sentinel-hub.com/api/latest/api/ogc/),
  * [FIS](https://www.sentinel-hub.com/develop/api/ogc/fis-request/),
  * authentication and rate-limit handling,

- geospatial utilities
  * interface for geospatial objects and transformations,
  * large area splitting,
  * data collection objects,
  * IO tools,

- download Sentinel-2 data from public [AWS S3 buckets](https://registry.opendata.aws/sentinel-2/)
  * restoration of .SAFE format,
  * L1C and L2A data,
  * command line interface,

- [Geopedia](http://portal.geopedia.world/) WMS and REST API.


## Documentation

For more information on the package and to access the documentation, visit [readthedocs](http://sentinelhub-py.readthedocs.io/).


## Examples

The package has a collection of Jupyter notebooks with examples. They are available in the [examples folder](https://github.com/sentinel-hub/sentinelhub-py/tree/master/examples) on GitHub and converted into documentation under [Examples section](https://sentinelhub-py.readthedocs.io/en/latest/examples.html).

Additionally, some examples are explained in Sentinel Hub webinar videos:

- [Process API in Python](https://www.youtube.com/watch?v=sX3w3Wd3FBw&list=PL46vEE2ks3tn8NGesSFllgJW5MSYRi4od&index=10&t=2220s)
- [OGC API in Python](https://www.youtube.com/watch?v=CBIlTOl2po4&list=PL46vEE2ks3tn8NGesSFllgJW5MSYRi4od&index=4&t=1766s)


## Blog posts

The package played a key role in many projects and use cases described at [Sentinel Hub blog](https://medium.com/sentinel-hub). The following blog posts are about the package itself:

 * [Upgrading the sentinelhub Python package](https://medium.com/sentinel-hub/upgrading-the-sentinelhub-python-package-2665f9c10df)
 * [Release of sentinelhub Python Package 2.0](https://medium.com/sentinel-hub/release-of-sentinelhub-python-package-2-0-a3d47709f8fd)


## Questions and Issues

Feel free to ask questions about the package and its use cases at [Sentinel Hub forum](https://forum.sentinel-hub.com/) or raise an issue on [GitHub](https://github.com/sentinel-hub/sentinelhub-py/issues).

You are welcome to send your feedback to the package authors, Sentinel Hub research team, through any of [Sentinel Hub communication channels](https://sentinel-hub.com/develop/communication-channels).


## License

See [LICENSE](https://github.com/sentinel-hub/sentinelhub-py/blob/master/LICENSE.md).

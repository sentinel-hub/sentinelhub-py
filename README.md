# Sentinel Hub Tools
[![Join the chat at https://gitter.im/sinergise/Lobby](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/sinergise/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/sinergise/sentinelhub.svg?branch=master)](https://travis-ci.org/sinergise/sentinelhub)

Python library of tools for easier use of Sentinel Hub products.

## Install

Library requires Python 2 version >=2.7 or Python 3. It can be installed using pip
```
pip install sentinelhub
```
or manually
```
cd sentinelhub
python setup.py build
python setup.py install
```

## Content

### AWS to SAFE
Tool for download Sentinel-2 data from [Sentinel-2 on AWS](http://sentinel-pds.s3-website.eu-central-1.amazonaws.com/) and reconstruction into ESA .SAFE format.

Functionalities:
 * Supports old and new (i.e. compact) .SAFE formats.
 * Requires either S-2 product ID or name and date of one tile inside the product.
 * Can either download entire product or only returns map of a .SAFE file structure.
 * Supports threaded download and redownloading existing data (not by default).

Examples:

```
sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551
```

```
sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 -i
```

```
sentinelhub.aws --tile T54HVH 2017-04-14 -e
```
For more functionalities check:
```
sentinelhub.aws --help
```

### Costum download tool
Tool used for download of data from any URL address into file with specified name.

Example:
```
sentinelhub.download http://sentinel.../metadata.xml MyFolder/example.xml
```

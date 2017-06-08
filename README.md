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

### AWS to SAFE tool

Tool for downloading Sentinel-2 data from [Sentinel-2 on AWS](http://sentinel-pds.s3-website.eu-central-1.amazonaws.com/) and reconstruction into ESA .SAFE format.

**Overview:**
 * Supports old and new (i.e. compact) .SAFE formats.
 * Requires either S-2 product ID or name and date of one tile inside the product.
 * Can either download entire product or only return a map of a .SAFE file structure.
 * Supports command line entries.
 * Supports threaded download and redownloading existing data (not by default).

**.SAFE format details**: </br>
Files in reconstructed .SAFE format follow the rules of [ESA naming convention](https://sentinel.esa.int/web/sentinel/user-guides/sentinel-2-msi/naming-convention). Reconstructed format differs from the original only in the following:
 * Folders *HTML* and *rep_info* inside main product folder do not contain any data.
 * Auxiliary file inside *AUX_DATA* folder of every tile in old .SAFE format does not have original name. Instead it is named *AUX_ECMWFT* which is the same as in compact .SAFE format.

**Functions:**
 * Function for downloading .SAFE format
 ```
 download_safe_format(product_id=None, tile=None, folder='.', redownload=False, threaded_download=False, entire_product=False)
 ```
 It can either take ID name of a product or name and date of a tile in form ```tile=(name, date)``` (e.g. ```tile=('T38TML','2015-12-19')```). </br>
 In case ```tile``` is specified and ```entire_product=True``` it will download entire product corresponding to that tile. Otherwise it will download only the tile.
 * Function for returning map structure of .SAFE format
 ```
 get_safe_format(product_id=None, tile=None, entire_product=False)
 ```
 It returns map in a form ```{'folder_name' : { 'subfolder_name' : { ... {'file_name': 'url_of_file_on_aws', ...}... }...}...}```.

**Examples:**
 * Download product
```
With Python
>>> sentinelhub.download_safe_format('S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551')
or with command line
$ sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551
```

 * Get .SAFE structure
```
With Python
>>> sentinelhub.get_safe_format('S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551')
or with command line
$ sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 -i
```

 * Download tile
 ```
 With Python
 >>> sentinelhub.download_safe_format(tile=('T38TML', '2015-12-19'))
 or with command line
 $ sentinelhub.aws --tile T54HVH 2017-04-14
 ```
 *(Please note that this specification is not always unique as there might be multiple tiles with the same name and date.)*

 * Download entire product corresponding to tile
 ```
 With Python
 >>> sentinelhub.download_safe_format(tile=('T38TML', '2015-12-19'), entire_product=True)
 or with command line
 $ sentinelhub.aws --tile T54HVH 2017-04-14 -e
 ```
 *(Please note that this specification is not always unique as there might be multiple tiles with the same name and date.)*

 * For more functionalities check:
```
$ sentinelhub.aws --help
```

### Custom download tool
Tool for downloading data from any URL address into file with specified name.

**Functions:**
```
sentinelhub.download_data(requstList, redownload=False, threaded_download=False)
```
where ```requestList = (url, filename)``` or ``` requestList = [(url1, filename1), (url2, filename2), ...]```

**Examples:**
 * Download an image from AWS using Python
```
With Python
>>> sentinelhub.download_data(('http://sentinel-s2-l1c.s3.amazonaws.com/tiles/54/H/VH/2017/4/14/0/B01.jp2', 'MyFolder/TestImage.jp2'))
or with command line
$ sentinelhub.download http://sentinel-s2-l1c.s3.amazonaws.com/tiles/54/H/VH/2017/4/14/0/B01.jp2 MyFolder/TestImage.jp2 -rt
```

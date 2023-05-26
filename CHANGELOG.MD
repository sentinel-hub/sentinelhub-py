## [Version 2.4.6] - 2018-11-27

- Added module `test_utils` with utilities for writing unit tests and adjusted to work with `sentinelhub-py` package.
- Added a new parameter to `OgcRequest.create_request` and `GeopadiaImageRequest.create_request` methods. By default this parameter will keep existing feature iterator class, if it exists, and therefore prevent re-running a request to services when re-calling `create_request` method.


## [Version 2.4.4] - 2018-10-23

- Added support for retrieving data from Geopedia vector layers.
- Temporal fix for handling rate limiting of Sentinel Hub services (HTTP response 429).


## [Version 2.4.3] - 2018-10-07

- Added the following new methods to `sentinelhub.BBox` class: `get_geojson`, `get_geometry`, `transform`.
- Added `reset` method to `sentinelhub.SHConfig` class and support for command line.
- Changed parameters of function `sentinelhub.opensearch.search_iter`.
- `sentinelhub.get_tile_info` now also accepts a time interval for `time` parameter.
- `sentinelhub.DownloadFailedException` message update.
- A few minor improvements.


## [Version 2.4.2] - 2018-08-28

- Fixed bug when reading locally stored 15-bit Jpeg 2000 images,
- Fixed issue when using `CustomUrlParam.GEOMETRY` in WGS84,
- Fixed issue in `AwsTile.tile_id_to_tile` with S2B products,
- `OgcRequest` classes can now also handle values of `time` parameter in `datetime.date` and `datetime.datetime` formats,
- More descriptive error message if Pillow fails to read Jpeg 2000 images,
- Minor other improvements


## [Version 2.4.1] - 2018-08-10

- Support for Python 3.7
- Updates regarding recent changes of access to Sentinel-2 data at AWS
- Those parameters from `config.json`, which are by default not specified, are now being preserved when package is reinstalled
- Some improvements of code and documentation


## [Version 2.3.0] - 2018-07-10

- Changed package dependency from `opencv-python` to `Pillow` package. This package is now being used to read and write PNG, JPEG and JPEG2000 images.
- Images with 3 or 4 channels in PNG, JPEG and JPEG2000 are now being read as RGB and not as BGR anymore. Comparing with previous package versions the values in 1st and 3rd channel are now swapped. There is no change in working with images in TIFF format.
- Minor fixes and improvements.


## [Version 2.2.0] - 2018-07-02

- Support for EOCloud data sources (Sentinel-1 GRD, Sentinel-3, Sentinel-5P, Landsat 5, Landsat 7, Landsat 8 L2A, Envisat MERIS, ...).
- If AWS credentials are not set in package config file they can be read from environmental variables.
- AWS files which will remain available through HTTP request will be always read from HTTP instead of S3. (e.g. `productInfo.json`, `tileInfo.json`, `metadata.xml`)
- Using sentinelhub.config command will produce more informative messages.


## [Version 2.1.0] - 2018-06-14

- Parameter `data_filter` added to `DataRequest.get_data` and `DataRequest.save_data` methods. It allows user to filter which data to download.
- Parameter `raise_download_errors` added to `DataRequest.get_data` and `DataRequest.save_data` methods. It is used to specify if errors in download process should raise `DownloadFailedException` or only a warning.
- New custom URL parameters `CustomUrlParam.BGCOLOR` and `CustomUrlParam.GEOMETRY` added.


## [Version 2.0.2] - 2018-05-22

- `opencv-python` version is now fixed to `3.4.0.14`, because newer version cannot read 15-bit jp2 files.
- Setup now contains a Python version requirement `>=3.5`


## [Version 2.0.1] - 2018-05-15

- Option to download Sentinel-2 L1C data from AWS using s3 instead of http.
- Warnings about change of coordinate order in WGS84 removed.
- Splitter classes now have an option to return a list of geometries.
- Fixed problem of saving data obtained with CustomUrlParam.EVALSCRIPT - filename is shortened, specific characters are removed
- Some updates of documentation


## [Version 2.0.0] - 2018-04-16

- For coordinate reference system WGS 84 the order of coordinates has changed from `lat, lng` to `lng, lat`.
- Added functionality for splitting large area polygons into smaller bounding boxes
- Package configuration file can now be accessed and modified via command line
- Added support for download from Requestor Pays AWS buckets
- Added functionality for downloading Sentinel-2 L2A data from AWS into .SAFE format
- Many minor code improvements


## [Version 1.1.1] - 2018-03-21

A stable version of `sentinelhub` package. Main improvements:
- added support for data sources Sentinel-2 L2A, Sentinel-1, Landsat 8, DEM and MODIS,
- added custom url parameters functionality,
- improved error handling,
- many improvements in user interaction,
- added support for Geopedia requests.

Note: this is the last version before `2.0.0`.


## [Version 1.0.0] - 2018-03-21

Version 1.0 allows users to use Sentinel-Hub OGC (WMS and WCS) web requests to download and process Sentinel-2 images within their Python scripts. It is backwards compatible with previous release, and therefore allows users to also download raw data from AWS to .SAFE format.


## [Version 0.1] - 2018-01-15

This version only supports downloading Sentinel-2 tiles from AWS. It works with both Python2 (>=2.7) and Python 3. [Sentinel-Hub](https://sentinel-hub.com/) account is not needed.

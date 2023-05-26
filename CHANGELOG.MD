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

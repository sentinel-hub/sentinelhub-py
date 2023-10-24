## [Version 3.9.2] - 2023-10-24

- Adjusted how user credentials are passed to the OAuth service.
- Added `QUALITY_FLAGS` band to `S3_OLCI`
- Batch statistical API now supports IAM role style credentials.
- Various minor improvements

## [Version 3.9.1] - 2023-05-04

- The parameter `sh_auth_base_url` has been replaced with `sh_token_url` to allow authentication on endpoints with suffixes other than `oauth/token`. For the new parameter the address must be provided in full, e.g. `https://services.sentinel-hub.com/oauth/token` instead of `https://services.sentinel-hub.com`. The change only affects users who manually adjusted this field.
- The package has been adjusted to fully support Python 3.11. In the process base class of  `DataCollection` was switched from `enum.Enum` to `aenum.Enum`, but we expect no behavioral changes.
- Various minor adjustments


## [Version 3.9.0] - 2023-04-04

This release focuses mostly on the changes regarding the Sentinel Hub configuration file (`SHConfig`) moving into the direction of a more common config usage:
- The format of the config file has been changed from JSON to TOML, which offers a more intuitive overview
- The config parameters can now be provided as input in the class constructor
- The location of the config file has been changed to `~/.config/sentinelhub/config.toml` on Linux/MacOS and `C:/Users/<USERNAME>/.config/sentinelhub/config.toml` on Windows, which means that the configs are now separated from the repo location
  - all environments where `sentinelhub-py` is installed now use the configuration from the same source
  - installing new versions now doesn't reset the configuration
- The option of having multiple configuration files was removed
- Profiles were introduced, allowing users to define and use multiple profiles defined in the same location
  - only the updated fields are present in the configuration, the rest are equal to the default configuration parameters
- A limited support of environment variables was introduced.
  - `SH_PROFILE` that dictates which profile should be used when not explicitly provided.
  - `SH_CLIENT_ID` and `SH_CLIENT_SECRET` for setting the SentinelHub credentials.
- The general precedence order of the provided parameters is `explicit parameters > environment > configuration file > defaults`.

For more information about the configuration updates, check the Configuration section in the [readthedocs](https://sentinelhub-py.readthedocs.io/en/latest/configure.html).

Other changes in this release include:
- Removed support for Python 3.7. This should help with an easier Conda installation
- The package is now released as a wheel
- Added support for [Async Process API](https://docs.sentinel-hub.com/api/latest/api/async-process/)
- Switched to a `pyproject.toml` based install, enabling a lighter package with fewer install files
- Increased tests performance and cleaned up deprecated functions


## [Version 3.8.4] - 2023-02-24

- Fixed a rate-limit bug that was introduced in version 3.8.2


## [Version 3.8.3] - 2023-02-16

- The properties `bbox` and `geometries` of `BatchRequest` no longer raise an exception and instead return `None` when not defined.
- `BatchSplitter` adjusted so it also works for batch requests that only have `bbox` specified.
- `BBoxCollection` has been marked as deprecated.
- The option to construct a `BBox` from strings, other `BBox` objects, and `shapely` geometries has been deprecated.
- The string representation of a `BBox` was marked to be changed in a future update. It will match the `repr` representation.
- Many unused minor functions were removed from `io_utils`, however `read_data` and `write_data` now support a few more formats.
- Some `url` fetching methods of `SHConfig` have been marked as deprecated (with hints for replacement).
- Large improvements to test suite.


## [Version 3.8.2] - 2023-01-31

- Generalized the `CRS` class to accept `fiona.crs.CRS` as input.
- Added warning to `SentinelHubBatch.reprocess_tile`, because the service endpoint will be removed.
- Deprecation warning is triggered for single request input to `DownloadClient.download`. In future versions only sequences of requests will be supported. A single request will need to be passed as a singleton list or tuple.
- Deprecated `PackageProps` class, will be removed in future version.
- Various improvements to code and tests.


## [Version 3.8.1] - 2023-01-18

- Added Harmonized Landsat Sentinel collection
- Due to recent changes the `aws` module no longer works reliably. We decided to stop maintaining it, but we updated the [AWS example notebook](https://github.com/sentinel-hub/sentinelhub-py/blob/master/examples/aws_request.ipynb) on workarounds for issues.
- Updated [BYOC example notebook](https://github.com/sentinel-hub/sentinelhub-py/blob/master/examples/byoc_request.ipynb) with information on manual configuration of collections.
- Updated [Large area utilities notebook](https://github.com/sentinel-hub/sentinelhub-py/blob/master/examples/large_area_utilities.ipynb) to show how one can use the obtained BBoxes to download data.
- Improved `BBoxSplitter` to allow splitting by size, contributed by @ColinMoldenhauer.
- Added `dataFilter` defaults to Batch Statistical (to match regular Statistical requests)
- Fixed OGC bug with parameters being encoded twice
- Module `type_utils` renamed to `types`
- Deprecated `os_utils` module
- Various minor code and test improvements


## [Version 3.8.0] - 2022-10-06

- (**codebreaking**) Switched to newly deployed Catalog 1.0.0. Main changes:
    * Slight changes in structure of results.
    * The `query` parameter is replaced with `filter`, which uses the CQL2 language for querying/filtering results.
- Added utility function `statistical_to_dataframe` for transforming results of (batch) statistical API to a dataframe. It is located in the new `sentinelhub.data_utils` module and requires the user to additionally install the `pandas` package.
- Fixed conditionals in batch client that caused issues when requesting tiling grid with id 0.
- Included an [example notebook](https://sentinelhub-py.readthedocs.io/en/latest/examples/reading_pu_from_headers.html) that describes how to obtain response headers. This can be used to access information about PU.
- Added pre-commit hooks to repository for easier development.
- Various minor improvements.


## [Version 3.7.0] - 2022-08-16

- Official support for [Sentinel Hub Batch Statistical API](https://docs.sentinel-hub.com/api/latest/api/batch-statistical/):
  * Implemented `SentinelHubBatchStatistical` and `BatchStatisticalRequest` classes to serve as an interface for the API.
  * Implemented `monitor_batch_statistical_job` and `monitor_batch_statistical_analysis` utility functions for monitoring batch statistical jobs.
  * Implemented `AwsBatchResults` utility class for efficient loading of batch results from an S3 bucke.
  * Prepared a [tutorial notebook](https://sentinelhub-py.readthedocs.io/en/latest/examples/batch_statistical.html).
  * Various minor improvements of batch-related functionalities and added more tests.
- Upgrade of download procedure to support better access to information about responses:
  * Implemented `DownloadResponse` object.
  * Changed the structure of `response.json` files in cached data to include info about response.
  * Changed the effect of `decode_data=False` parameter that is passed to `DownloadClient.download` and `DataRequest.get_data` methods. It used to provide raw response in bytes but now it provides instances of `DownloadResponse` object containing raw response, response headers, and more information about a response.
  * Larger changes in the internal structure of download clients.
- Parameter `show_progress` is now available for all `DataRequest.get_data` and `DataRequest.save_data` method calls.
- Added `SHConfig.__eq__` method that compares object values.
- Added `BBox.apply`, `Geometry.apply`, and `BBoxCollection.apply` methods.
- Improved handling of User-Agent headers - `SentinelHubDownloadClient` now always includes them.
- Removed references to rate limit headers that don't exist anymore from `SentinelHubRateLimit`.
- Changed how the request payer is configured in `AwsDownloadClient` - parameter `RequestPayer="requester"` is not hardcoded anymore.
- Removed deprecated `DataSource` class, old Landsat `DataCollection` names, and `base_url` parameter.
- Various minor improvements in code, tests, and documentation.


## [Version 3.6.4] - 2022-07-27

- Increased minimal `Pillow` version to `9.2.0` and removed a fallback to `rasterio` when decoding JP2 images.
- Various minor improvements in docstrings, example notebooks, and tests.


## [Version 3.6.3] - 2022-06-21

- Fixed and issue with missing `bufferX` and `bufferY` parameters in `BatchSplitter`.
- Updated minimal version of `requests` package in requirements to avoid issues with failed `JSONDecodeError` import.
- Added `flake8` style checker for code and notebooks and `isort` formatting for notebooks.


## [Version 3.6.2] - 2022-06-13

- `BatchSplitter` is now returning bounding boxes with a correct buffer and without numerical errors. This change is **code-breaking**.
- Fixed an issue where failed authentication to Sentinel Hub would raise a misleading `MissingTokenError`. Now a correct error is being raised.
- Improved parsing of error messages obtained from Sentinel Hub.
- A few minor improvements.


## [Version 3.6.1] - 2022-06-03

- Improvements of Sentinel Hub authentication session sharing:
  * `sentinelhub.download.SessionSharingThread` can now handle previously undeleted shared memory files and warn about them.
  * Added a high-level utility `sentinelhub.download.SessionSharing` that ensures thread joining at the end. The tutorial notebook has been updated accordingly.
- Minor Pylint updates.


## [Version 3.6.0] - 2022-05-27

- Official support for sharing Sentinel Hub authentication session between parallel Python processes:
  * improved interfaces of `SentinelHubSession` and `SentinelHubDownloadClient`,
  * implemented utilities `sentinelhub.download.SessionSharingThread` and `sentinelhub.download.collect_shared_session` for sharing session with standard Python multiprocessing,
  * added a [tutorial notebook](https://sentinelhub-py.readthedocs.io/en/latest/examples/session_sharing.html) explaining the process and providing prototype implementations for different parallelization frameworks.
- The package is now fully typed and `mypy` checks are performed in CI.
- Refactored internal functionalities of `SHConfig` class.
- `DownloadFailedException` now carries an optional instance of `requests.RequestException`.
- Removed support for an old prototype EOCloud deployment of Sentinel Hub OGC services.
- Documentation build improvements - package content pages are now being auto-generated. Some documentation links changed because of that.
- Various minor improvements and fixes. Contributions by @sebastianclarke and @apazga


## [Version 3.5.2] - 2022-05-03

- Fixed an issue with parsing WGS84 in `sentinelhub.CRS` class.
- Fixed an issue with using `SHConfig` class in Ray workers. A few minor changes in `SHConfig` class.
- Renamed `sentinelhub.geometry.BaseGeometry` to a private class `sentinelhub.geometry._BaseGeometry`.
- Added type annotations for some modules.


## [Version 3.5.1] - 2022-04-21

- New enums `MosaickingOrder` and `ResamplingType` with better parsing of parameters for mosaicking order, downsampling, and upsampling.
- `BBox.buffer` method now has an option to apply an absolute buffer. Default is still a relative buffer.
- Added `monitor_batch_analysis` utility function.
- Added `ByocCollectionBand` dataclass for better deserialization of payloads from Sentinel Hub BYOC service.
- Added `SentinelHubSession.info` method for obtaining session token information.
- Increased cache size for `CRS.projection`, `CRS.pyproj_crs`, and `CRS.get_transform_function` methods.
- Fixed an issue with a progress bar in download clients.
- Added `py.typed` file.
- Various minor improvements in code and documentation.


## [Version 3.5.0] - 2022-03-24

- Larger change of internal package structure:
  * some modules were grouped into subfolders `api`, `aws`, `download`, and `geopedia`,
  * package dependencies for working with AWS are not installed by default anymore, instead they require `sentinelhub[AWS]` extension,
  * AWS functionalities can now be imported only from `sentinelhub.aws`
  * documentation and tests were updated to reflect these changes.
- Downloading of Sentinel-2 data from AWS into .SAFE format was updated. Now all baselines, including `04.00`, are correctly supported. Contributions by @anthony-scarth and @sebastianclarke.
- Dependency `Pillow>=9.0.0` is now officially supported. Because new versions incorrectly read 15-bit JPEG2000 images a warning is raised in such cases. The warning suggests to install `rasterio` which in `sentinelhub-py` now has priority over `Pillow` for reading JPEG2000 format.
- Fixed an issue in DownloadClient cache checking.
- Applied `black` and `isort` formatting on the entire repository.
- Various minor updates in the code and documentation.


## [Version 3.4.4] - 2022-02-16

- Improved handling of AWS credentials
- Corrected LandSat units for bands with surface temperature
- Added deprecation warning for Fis service
- Various minor improvements


## [Version 3.4.3] - 2022-01-06

- Dropped support for Python `3.6` and added support for Python `3.10`.
- Added a few new `MimeType` enums constants and `MimeType.matches_extension` method.


## [Version 3.4.2] - 2021-12-06

- `TileSplitter` utility now works for any `DataCollection` enum. Its implementation changed from using Sentinel Hub WFS to using Sentinel Hub Catalog to collect data about tiles. Consequentially, some initialization parameters and some output information changed. These changes are **code-breaking**.
- A couple of fixes in definitions of meta-bands for a few `DataCollection` enums.


## [Version 3.4.1] - 2021-10-27

- **Code-breaking** change of attribute `bands` of class `DataCollection`. Switched from a list of band names to a list of band information (name, units, output types). `DataCollection` now also contains information about metabands (e.g. `dataMask`).
- Fixed an issue with false hash collision detection
- The `time` parameter in `SentinelHubCatalog` is now optional
- `SentinelHubBYOC` now provides `reingest_tile` method
- Minor fixes and improvements


## [Version 3.4.0] - 2021-09-10

- **Code-breaking** change of Sentinel Hub Batch API interface (i.e. `SentinelHubBatch`). Check updated [example notebook](https://sentinelhub-py.readthedocs.io/en/latest/examples/batch_processing.html) for more info. Related changes:
  * support for Batch collections endpoints,
  * support for endpoint to update a batch request,
  * added utility function `monitor_batch_job`,
  * **code-breaking** change of `BatchSplitter` class.
- Renamed Landsat `DataCollection` enums to be in line with Sentinel Hub service. Old names are deprecated.
- Better support for data fusion and added an example in [documentation](https://sentinelhub-py.readthedocs.io/en/latest/examples/data_collections.html#Data-fusion).
- Official support for Python 3.9.
- Logging improvements:
  * more logging in download procedure,
  * added a [documentation page](https://sentinelhub-py.readthedocs.io/en/latest/logging.html) about logging.
- Download improvements:
  * option to show progress bar during download,
  * checking for hash collisions of cached downloaded data.
- Option to hide credentials from `SHConfig` representation.
- `SentinelHubCatalog` now correctly supports searching by collection id.
- Change representation of geometry objects in notebooks.
- Removed various functions and parameters which have been deprecated in earlier versions.
- Converted all package tests from `unittest` to `pytest`.
- A larger update of package Readme.
- Minor fixes and improvements.


## [Version 3.3.2] - 2021-07-19

- Added new data collection enums:
  * `DataCollection.LANDSAT7_L1`,
  * `DataCollection.LANDSAT7_L2`,
  * `DataCollection.LANDSAT15_L1`.
- Fixed an issue where user-defined `DataCollection` objects couldn't be passed to a distributed environment.
- Fixed an issue with projections from uncommon CRS and added an optional `always_xy` parameter.
- Improvements in the [documentation notebook about Sentinel Hub BYOC](https://sentinelhub-py.readthedocs.io/en/latest/examples/byoc_request.html).
- Minor improvements in code style and documentation.


## [Version 3.3.1] - 2021-06-15

- Added new data collection enums:
  - `DataCollection.LANDSAT8_L1`,
  - `DataCollection.LANDSAT8_L2`,
  - `DataCollection.LANDSAT45_L1`,
  - `DataCollection.LANDSAT45_L2`.
- Added support for downloading Sentinel-2 L2A products with a special baseline `00.01` from AWS. Contributed by @marcosdejesus
- Added an optional `offset` parameter to `UtmGridSplitter` and `UtmZoneSplitter`.
- Minor updates and fixes to support changes of Sentinel Hub services.
- Various documentation and code style improvements and fixes.


## [Version 3.3.0] - 2021-05-12

- Added support for [Sentinel Hub Statistical API](https://docs.sentinel-hub.com/api/latest/api/statistical/) and a [tutorial notebook](https://sentinelhub-py.readthedocs.io/en/latest/examples/statistical_request.html) explaining how to use it.
- Minor code and documentation updates.


## [Version 3.2.1] - 2021-03-26

- Added new data collection enums:
  * `DataCollection.DEM_MAPZEN`,
  * `DataCollection.DEM_COPERNICUS_30`,
  * `DataCollection.DEM_COPERNICUS_90`
- `SentinelHubCatalog.search` now correctly uses filters defined on `DataCollection` objects
- Fixed a bug in `GeopediaFeatureIterator`
- Various documentation improvements


## [Version 3.2.0] - 2021-03-05

- Added support for [Sentinel Hub Catalog API](https://docs.sentinel-hub.com/api/latest/api/catalog/) and a [tutorial notebook](https://sentinelhub-py.readthedocs.io/en/latest/examples/data_search.html) explaining how to use it.
- Added support for [Sentinel Hub BYOC API](https://docs.sentinel-hub.com/api/latest/api/byoc/) and a [tutorial notebook](https://sentinelhub-py.readthedocs.io/en/latest/examples/byoc_request.html) explaining how to use it.
- Removed enums `MimeType.TIFF_d8`, `MimeType.TIFF_d16`, and `MimeType.TIFF_d32f` as they are not supported by Sentinel Hub service anymore. Use `MimeType.TIFF` instead.
- Removed enums `CustomUrlParam.ATMFILTER`, `CustomUrlParam.TRANSPARENT`, and `CustomUrlParam.BGCOLOR` as they are not supported by Sentinel Hub service anymore.
- Improved how time parameters are handled in the package and refactored `time_utils.py` module.
- Various documentation improvements, better [instructions about configuring Sentinel Hub credentials](https://sentinelhub-py.readthedocs.io/en/latest/configure.html#sentinel-hub-configuration).
- Added methods `BBox.transform_bounds` and `Geometry.from_geojson`.
- Unified implementation of feature iterators throughout the package.
- Various minor improvements and fixes.


## [Version 3.1.0] - 2020-10-19

- Added support for [**Sentinel Hub Batch service**](https://docs.sentinel-hub.com/api/latest/api/batch/) and [a tutorial notebook](https://sentinelhub-py.readthedocs.io/en/latest/examples/batch_processing.html) explaining how to use it.
- Changed "data source" terminology to "data collection" to keep in sync with the Sentinel Hub service. Because of that `DataSource` has been renamed to `DataCollection` and `data_source` parameters renamed to `data_collection`. The old names became deprecated.
- Large improvements in the implementation of `DataCollection` class (i.e. ex `DataSource` class) and supporting different deployments of Sentinel Hub service. Added [a tutorial notebook](https://sentinelhub-py.readthedocs.io/en/latest/examples/data_collections.html) explaining how to use the new `DataCollection` functionalities.
- Improved parameter handling in `SentinelHubRequest.input_data` method. Added optional parameters `upsampling` and `downsampling`.
- Deprecated `sentinelhub.download.client.get_json` function and replaced it with a method `sentinelhub.download.client.DownloadClient.get_json`.
- A `config.json` is now being generated on-the-fly instead of being committed in the repository.
- Minor improvements and fixes.


## [Version 3.0.5] - 2020-08-03

- A minor enhancement of `other_args` parameter in `SentinelHubRequest.input_data` method.


## [Version 3.0.4] - 2020-05-24

- Updated documentation examples
  * Improved [examples](https://sentinelhub-py.readthedocs.io/en/latest/examples/processing_api_request.html) of how to use Sentinel Hub [Processing API service](https://docs.sentinel-hub.com/api/latest/#/API/),
  * Deprecated [examples](https://sentinelhub-py.readthedocs.io/en/latest/examples/ogc_request.html) of how to use Sentinel Hub OGC service
- Fixed an issue of `SentinelHubRequest` not using `config` parameter correctly.


## [Version 3.0.3] - 2020-05-02

- A fix for accessing Sentinel-5p data from Creodias


## [Version 3.0.2] - 2020-03-04

- Added a notebook with examples of how to use the `SentinelHubRequest` interface of Sentinel Hub Processing API. The notebook is also available in [documentation](https://sentinelhub-py.readthedocs.io/en/latest/examples/sentinelhub_request.html).
- OGC requests are now sending additional parameter `warnings=false` to better catch errors when requesting a too large image. (Note: this changes filenames of locally stored data.)
- Improvements in when a session is created in the download procedure.
- Improvements in `CRS` class, added `CRS.pyproj_crs` method.


## [Version 3.0.1] - 2020-02-26

- `SentinelHubDownloadClient` is now caching `SentinelHubSession` instances,
- `SentinelHubSession` now uses retry logic on failed authentication requests,
- removed unnecessary warnings about credentials,
- added handling of `requests.exceptions.ChunkedEncodingError`.


## [Version 3.0.0] - 2020-02-17

- Large changes in Sentinel Hub rate-limiting object that improve download performance.
- Added `SentinelHubRequest` class as an interface class for [Sentinel Hub API](https://docs.sentinel-hub.com/api/latest/#/API/).
- Changed the filename convention of downloaded data.
- Improvements in download procedure and `DownloadRequest` class.
- Added support for Python 3.8, dropped support for Python 3.5.
- `DataRequest` classes now work with an optional parameter `config` instead of `instance_id`.
- `sentinelhub.CRS` now enables using any custom CRS.
- Resolved `pyproj` warnings.
- Removed deprecated methods of `BBox` and `Geometry` classes and `transform_bbox` function.
- Various other improvements and fixes.


## [Version 3.0.0b1] - 2019-11-18

The main changes:
- Support for new Sentinel Hub API
- Download utilities refactored
- Added an algorithm for handling rate-limiting when downloading from Sentinel Hub service
- A new procedure of caching downloaded data
- Changes of parameters in `config.json`

More will be added soon.


## [Version 2.6.1] - 2019-11-11

- Added support for custom Sentinel Hub BYOC data sources
- Sentinel Hub `FisRequest` now collects data with POST requests instead of GET requests


## [Version 2.6.0] - 2019-07-31

- Added support for Sentinel-1 orbit direction ([code example](https://sentinelhub-py.readthedocs.io/en/latest/examples/ogc_request.html#Example-16:-Sentinel-1,-ascending-orbit-direction)).
- Added new utilities for splitting large areas - `UtmGridSplitter` and `UtmZoneSplitter` ([code example](https://sentinelhub-py.readthedocs.io/en/latest/examples/large_area_utilities.html#Splitting-into-UTM-grid-zones)).
- Renamed module `test_utils` to `testing_utils`.
- Added code coverage.


## [Version 2.5.3] - 2019-05-07

- Performance improvements of geometrical transformations with `pyproj`. Using `pyproj>=2.0.0` should now be as fast as with older versions of `pyproj`.
- Structural changes of `sentinelhub.CRS` class.
- Extended support for downloading Sentinel-2 data into .SAFE format. Now supporting L1C baselines up to 02.07 and L2A up to 02.11. Contributed by @rouault
- Fixed bug in `sentinelhub.BBox.get_transform_vector` method, contributed by @ds2268
- Fixed bug in `sentinelhub.opensearch.get_tile_info` function, contributed by @dhepper
- Minor improvements in code and documentation


## [Version 2.5.1] - 2019-02-14

- Improvements of `GeopediaSession` class:
    - support for authentication (login with Geopedia credentials)
    - storing all session info
- `GeopediaImageRequest` and `GeopediaFeatureIterator` now have an optional parameter for specifying a custom instance of `GeopediaSession`. Therefore they can access private Geopedia layers.


## [Version 2.5.0] - 2019-02-01

- Added support for Sentinel Hub Feature Info Service (FIS). It is available with `FisRequest` class.
- Added notebook with examples how to use `FisRequest` - [link](https://sentinelhub-py.readthedocs.io/en/latest/examples/fis_request.html)
- Module `sentinelhub.common` renamed to `sentinelhub.geometry`, added `Geometry` and `BBoxCollection` classes, introduced properties
- **Code breaking change**: `sentinelhub.BBox.transform` method does not change the object itself anymore but instead returns a copy of the object in new CRS.
- Deprecated `transform_bbox` utility function as `sentinelhub.BBox.transform` can be used instead
- Added `bbox_to_dimensions` utility function to `geo_utils` module. Contributed by @daviddemeij
- Added `CustomGridSplitter` and new examples about it in [Large area utilities notebook](https://sentinelhub-py.readthedocs.io/en/latest/examples/large_area_utilities.html)
- Fixed problems with `numpy` version `1.16.0`.


## [Version 2.4.7] - 2018-12-10

- `DataSource` class does not have subclasses anymore and therefore can be pickled and used during multiprocessing
- `GeopediaFeatureIterator` now has an optional `query_filter` init parameter and method `get_size()`


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

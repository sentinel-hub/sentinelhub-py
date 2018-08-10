=====================================================
Downloading satellite data from AWS with command line
=====================================================

The following examples show how to download Sentinel-2 data from AWS S3 storage bucket and store them into original
`ESA .SAFE format`_. Before testing any of the examples below please check
`Configuration paragraph <configure.html#amazon-s3-capabilities>`_ for details about configuring AWS credentials
and information about charges.


Sentinel-2 products
*******************

To download a Sentinel-2 product and save it to present working directory only ESA product ID has to be specified::

$ sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551

Download of L2A products works in the same way, as the difference between L1C and L2A can be determined from product ID::

$ sentinelhub.aws --product S2A_MSIL2A_20180402T151801_N0207_R068_T33XWJ_20180402T202222

If certain file has already been downloaded and exists in the expected folder it by default won't be redownloaded.
However redownload can be specified with the following flag::

$ sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 -r

It is possible to get only information about .SAFE structure (without charge) and not download the product::

$ sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 -i

Download product and save it to specific file directory::

$ sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 -f /home/ESA_Products

Download specified bands only::

$ sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 --bands B08,B11


Sentinel-2 tiles
****************

It is possible to download only a specific tile within the product. The tile can be specified with tile name
(i.e. tile's spatial location) and sensing date::

$ sentinelhub.aws --tile T54HVH 2017-04-14

By default L1C tile data will be downloaded. In order to download L2A tile data a flag has to be used::

$ sentinelhub.aws --tile T33XWJ 2018-04-02 --l2a

Download entire product corresponding to tile::

$ sentinelhub.aws --tile T54HVH 2017-04-14 -e


.SAFE format details
********************

Because files in AWS bucket are stored differently the *sentinelhub* package has to reconstruct .SAFE format following
the rules of ESA naming convention. Reconstructed format may differ from the original only in the following:

* Folders HTML and rep_info inside main product folder do not contain any data.
* Auxiliary file inside AUX_DATA folder of every tile in old .SAFE format does not have original name. Instead it is named AUX_ECMWFT which is the same as in compact .SAFE format.
* Some products created in October 2017 might miss quality report files (`FORMAT_CORRECTNESS.xml`, `GENERAL_QUALITY.xml`, `GEOMETRIC_QUALITY.xml`, `RADIOMETRIC_QUALITY.xml` and `SENSOR_QUALITY.xml`).

If you notice any other difference please raise an issue at
`GitHub page <https://github.com/sentinel-hub/sentinelhub-py/issues>`_.


.. _`ESA .SAFE format`: https://sentinel.esa.int/web/sentinel/user-guides/sentinel-2-msi/data-formats

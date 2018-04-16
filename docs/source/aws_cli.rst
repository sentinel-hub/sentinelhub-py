==========================================
How to download AWS data from command line
==========================================

These examples show how to download Sentinel-2 data from Sentinel-2 on AWS to ESA SAFE format.

Sentinel-2 L1C products
***********************

Download product and save to working directory::

$ sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551

Get .SAFE structure only::

$ sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 -i

Download product and save to specific directory::

$ sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 -f /home/ESA_Products

Download specified bands only::

$ sentinelhub.aws --product S2A_MSIL1C_20170414T003551_N0204_R016_T54HVH_20170414T003551 --bands B08,B11

Download tile::

$ sentinelhub.aws --tile T54HVH 2017-04-14

Download entire product corresponding to tile::

$ sentinelhub.aws --tile T54HVH 2017-04-14 -e

Download an image from AWS::

$ sentinelhub.download http://sentinel-s2-l1c.s3.amazonaws.com/tiles/54/H/VH/2017/4/14/0/metadata.xml home/example.xml

Sentinel-2 L2A products
***********************

Sentinel-2 L2A products are located at AWS Requester Pays bucket. In order to download these products AWS access key has
to be saved in the package configuration file as shown in `Configuration paragraph <configure.html#amazon-s3-capabilities>`_.

The download of L2A products works the same as download of L1C products::

$ sentinelhub.aws --product S2A_MSIL2A_20180402T151801_N0207_R068_T33XWJ_20180402T202222

To download data by specifying the tile an additional flag has to be used::

$ sentinelhub.aws --tile T33XWJ 2018-04-02 --l2a


.SAFE format details
********************

Files in reconstructed .SAFE format follow the rules of ESA naming convention. Reconstructed format differs from the
original only in the following:

* Folders HTML and rep_info inside main product folder do not contain any data.
* Auxiliary file inside AUX_DATA folder of every tile in old .SAFE format does not have original name. Instead it is named AUX_ECMWFT which is the same as in compact .SAFE format.
* Some products created in October 2017 might miss quality report files (`FORMAT_CORRECTNESS.xml`, `GENERAL_QUALITY.xml`, `GEOMETRIC_QUALITY.xml`, `RADIOMETRIC_QUALITY.xml` and `SENSOR_QUALITY.xml`).

If you notice any other difference please raise an issue at
`GitHub page <https://github.com/sentinel-hub/sentinelhub-py/issues>`_.

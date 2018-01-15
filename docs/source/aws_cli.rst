How to download AWS data from command line
==========================================

These examples show how download Sentinel-2 data from Sentinel-2 on AWS to ESA SAFE format. Download uses multiple threads.

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

.SAFE format details
--------------------

Files in reconstructed .SAFE format follow the rules of ESA naming convention. Reconstructed format differs from the original only in the following:

* Folders HTML and rep_info inside main product folder do not contain any data.
* Auxiliary file inside AUX_DATA folder of every tile in old .SAFE format does not have original name. Instead it is named AUX_ECMWFT which is the same as in compact .SAFE format.


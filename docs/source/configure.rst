*************
Configuration
*************

The package contains a configuration file ``config.json``. After the package is installed you can check the initial
configuration parameters in command line::

$ sentinelhub.config --show

Sentinel Hub Capabilities
*************************

The ``instance_id`` parameter will be empty. In case you would like to use any capabilities of the package that
interact with `Sentinel Hub services`_ you can set your Sentinel Hub instance ID with::

$ sentinelhub.config --instance_id <your instance id>

By doing so the package will use this instance ID to interact with Sentinel Hub unless you purposely specify a
different one in the code.

Amazon S3 Capabilities
**********************

The ``aws_access_key_id`` and ``aws_secret_access_key`` will be empty. In case you would like package to access
Sentinel-2 L2A data with `Amazon S3`_ service you can set these values using your AWS access key::

$ sentinelhub.config --aws_access_key_id <your access key> --aws_secret_access_key <your secret access key>

Sentinel-2 L1C data is by default being downloaded with free of charge http service. However soon this option will not
be available anymore. Therefore it is also possible to download L1C data with S3 service by first setting
``use_s3_l1c_bucket`` parameter to ``true``::

$ sentinelhub.config --use_s3_l1c_bucket true

**Note:** Satellite data at S3 is contained in Requester Pays buckets. Therefore Amazon will charge download of such data
according to `Amazon S3 Pricing`_.


Other
*****

For more configuration options check::

$ sentinelhub.config --help


.. _`Sentinel Hub services`: https://www.sentinel-hub.com/develop/documentation/api/ogc_api/
.. _`Amazon S3`: https://aws.amazon.com/s3/
.. _`Amazon S3 Pricing`: https://aws.amazon.com/s3/pricing/?p=ps

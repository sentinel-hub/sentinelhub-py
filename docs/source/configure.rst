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

The ``aws_access_key_id`` and ``aws_secret_access_key`` will also be empty. In case you would like package to access
Sentinel-2 L2A data from `Amazon S3`_ service you can set these values using your AWS access key::

$ sentinelhub.config --aws_access_key_id <your access key> --aws_secret_access_key <your secret access key>

Other
*****

For more configuration options check::

$ sentinelhub.config --help


.. _`Sentinel Hub services`: https://www.sentinel-hub.com/develop/documentation/api/ogc_api/
.. _`Amazon S3`: https://aws.amazon.com/s3/

*************
Configuration
*************

The package contains a configuration file ``config.json``. After the package is installed you can check the initial
configuration parameters in command line::

$ sentinelhub.config --show

The ``instance_id`` parameter will be empty. In case you would like to use any functionalities of the package that
interact with `Sentinel Hub services`_ you can set your Sentinel Hub instance ID with::

$ sentinelhub.config --instance_id <your instance id>

By doing so the package will use this instance ID to interact with Sentinel Hub unless you purposely specify a
different one in the code.

For more configuration options check::

$ sentinelhub.config --help


.. _`Sentinel Hub services`: https://www.sentinel-hub.com/develop/documentation/api/ogc_api

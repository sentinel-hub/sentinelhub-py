*************
Configuration
*************

The package contains a configuration file ``config.json``. After the package is installed you can check the initial
configuration parameters in command line::

$ sentinelhub.config --show

Sentinel Hub Capabilities
*************************

By default parameters ``instance_id``, ``sh_client_id`` and ``sh_client_secret`` will be empty. In case you would like
to use any capabilities of the package that interact with `Sentinel Hub services`_ you can set any of these parameters
with::

$ sentinelhub.config --instance_id <your instance id>
$ sentinelhub.config --sh_client_id <your client id> --sh_client_secret <your client secret>

By doing so the package will use these parameters to interact with Sentinel Hub unless you purposely specify an
instance of ``sentinelhub.SHConfig`` object containing different parameters.

Amazon S3 Capabilities
**********************

The package enables downloading Sentinel-2 L1C and L2A data from `Amazon S3`_ storage buckets. The data is contained in
Requester Pays buckets therefore `AWS credentials`_ are required to use these capabilities. The credentials
can be set in package's configuration file with parameters ``aws_access_key_id`` and ``aws_secret_access_key``. This can
be configured from command line::

$ sentinelhub.config --aws_access_key_id <your access key> --aws_secret_access_key <your secret access key>

In case the credentials are not set, the package will instead automatically try to use **locally stored AWS credentials**,
if they were configured according to `AWS configuration instructions`_. Any other configuration parameters (e.g. region)
will also be collected the same way.

The AWS credentials also have to have correct permissions to be able to download data from S3 buckets.
That can be configured in AWS IAM console. There are many ways how to configure sufficient permission, one of them
is setting them to *AmazonS3ReadOnlyAccess*.

**Important:** Because satellite data at S3 is contained in Requester Pays buckets Amazon will charge users for
download according to `Amazon S3 Pricing`_. In this case users are charged for amount of data downloaded and the number
of requests. The *sentinelhub* package will make at most one GET request for each file downloaded. Files *metadata.xml*,
*tileInfo.json* and *productInfo.json* will be obtained without any charge from `Sentinel Hub public repository`_.

Other
*****

For more configuration options check::

$ sentinelhub.config --help


.. _`Sentinel Hub services`: https://www.sentinel-hub.com/develop/documentation/api/ogc_api/
.. _`Amazon S3`: https://aws.amazon.com/s3/
.. _`AWS credentials`: https://docs.aws.amazon.com/general/latest/gr/aws-security-credentials.html
.. _`AWS configuration instructions`: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
.. _`Amazon S3 Pricing`: https://aws.amazon.com/s3/pricing/?p=ps
.. _`Sentinel Hub public repository`: https://roda.sentinel-hub.com/sentinel-s2-l1c/

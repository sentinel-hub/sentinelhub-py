*************
Configuration
*************


The package contains a configuration file ``config.json``. After the package is installed you can check the initial
configuration parameters using the command line::

$ sentinelhub.config --show

The same configuration can be seen by instantiating an instance of :class:`~sentinelhub.config.SHConfig`;
without passing parameters the ``config.json`` will be used to populate the values:

.. code-block:: python

    from sentinelhub import SHConfig

    config = SHConfig()

    config

    > SHConfig(
    >    instance_id='',
    >    sh_client_id='',
    >    sh_client_secret='',
    >    aws_access_key_id='',
    >    aws_secret_access_key='',
    >    ...
    > )


Sentinel Hub Configuration
**************************


In order to use Sentinel Hub services you will need a Sentinel Hub account. If you do not have one yet, you can
create a free trial account at `Sentinel Hub`_. If you are a researcher you can even apply for a free non-commercial
account at `ESA OSEO page`_. The following configurations are then linked to your account.

By default parameters ``instance_id``, ``sh_client_id`` and ``sh_client_secret`` will be empty.

Parameter ``instance_id`` is used when using OGC endpoints of the `Sentinel Hub services`_. It is the identifier of a
configuration users can set up in the `Sentinel Hub Dashboard`_ under "Configuration Utility".

The ``sh_client_id`` and ``sh_client_secret`` parameters can also be created in the `Sentinel Hub Dashboard`_ under
"User settings". The two parameters are needed when accessing protected endpoints of the service (Process, Catalog,
Batch, BYOC, and other APIs). There is "OAuth clients" frame where we can create a new OAuth client.

You can set any of these parameters with::

$ sentinelhub.config --instance_id <your instance id>
$ sentinelhub.config --sh_client_id <your client id> --sh_client_secret <your client secret>

or set them up by configuring an instance of :class:`~sentinelhub.config.SHConfig`:

.. code-block:: python

    from sentinelhub import SHConfig

    config = SHConfig()

    config.instance_id = '<your instance id>'
    config.sh_client_id = '<your client id>'
    config.sh_client_secret = '<your client secret>'


One can save these into the package ``config.json`` file by calling:

.. code-block:: python

    config.save()

Once set (either with command line or as described above), the default parameters to interact with Sentinel Hub
will be read from ``config.json``, unless you purposely specify an instance of :class:`~sentinelhub.config.SHConfig`
object containing different parameters.

.. admonition:: Additional information on creating OAuth client

    For detailed instructions on how to obtain credentials, you can see the `Sentinel Hub webinar`_.



Amazon S3 Configuration
***********************

The package enables downloading Sentinel-2 L1C and L2A data from `Amazon S3`_ storage buckets. The data is contained in
Requester Pays buckets therefore `AWS credentials`_ are required to use these capabilities. The credentials
can be set in the package configuration file with parameters ``aws_access_key_id`` and ``aws_secret_access_key``. This
can be configured using the command line as::

$ sentinelhub.config --aws_access_key_id <your access key> --aws_secret_access_key <your secret access key>

or again as above:

.. code-block:: python

    from sentinelhub import SHConfig

    config = SHConfig()

    config.aws_access_key_id = '<your access key>
    config.aws_secret_access_key = '<your secret access key>'


possibly storing this information into the package ``config.json`` file (for simpler re-use) by calling:

.. code-block:: python

    config.save()

In case the credentials are not set, the package will instead automatically try to use **locally stored AWS credentials**,
if they were configured according to `AWS configuration instructions`_. Any other configuration parameters (e.g. region)
will also be collected in the same way.

The AWS account must have correct permissions set up to be able to download data from S3 buckets.
That can be configured in AWS IAM console. There are many ways how to configure sufficient permission, one of them
is setting them to *AmazonS3ReadOnlyAccess*.

.. warning::

    Because Sentinel-2 satellite data on S3 is contained in Requester Pays buckets Amazon will charge users for
    download according to `Amazon S3 Pricing`_. In this case users are charged for amount of data downloaded and
    the number of requests. The *sentinelhub* package will make at most one GET request for each file downloaded.
    Files *metadata.xml*, *tileInfo.json* and *productInfo.json* will be obtained without any charge from
    `Sentinel Hub public repository`_.


Other configuration options
***************************

For more configuration options check::

$ sentinelhub.config --help


.. _`Sentinel Hub`: https://www.sentinel-hub.com/trial
.. _`ESA OSEO page`: https://earth.esa.int/aos/OSEO
.. _`Sentinel Hub Dashboard`: https://apps.sentinel-hub.com/dashboard/
.. _`Sentinel Hub services`: https://www.sentinel-hub.com/develop/documentation/api/ogc_api/
.. _`Sentinel Hub webinar`: https://www.youtube.com/watch?v=CBIlTOl2po4&t=1760s
.. _`Amazon S3`: https://aws.amazon.com/s3/
.. _`AWS credentials`: https://docs.aws.amazon.com/general/latest/gr/aws-security-credentials.html
.. _`AWS configuration instructions`: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
.. _`Amazon S3 Pricing`: https://aws.amazon.com/s3/pricing/?p=ps
.. _`Sentinel Hub public repository`: https://roda.sentinel-hub.com/sentinel-s2-l1c/

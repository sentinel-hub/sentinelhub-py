Configuration
=============

Part of the package configuration is represented by the :class:`~sentinelhub.config.SHConfig` class. It can be adjusted and passed to most of the functions and constructors of the ``sentinelhub`` package to either provide credentials or modify behavior such as the number of download processes.

.. code-block:: python

    from sentinelhub import SHConfig

    config = SHConfig()

    config

    > SHConfig(
    >    instance_id='',
    >    sh_client_id='',
    >    sh_client_secret='',
    >    sh_base_url='https://services.sentinel-hub.com',
    >    sh_auth_base_url='https://services.sentinel-hub.com',
    >    ...
    > )

To avoid the need of constant reconfiguration in the code, we also support adjusting the values in a ``config.toml`` file.

Configuration File
******************

Whenever a new ``SHConfig`` object is created, the default values of the fields are updated with the contents of the configuration file. The configuration file also supports multiple profiles, which can be used with ``SHConfig("myprofile")``. If no profile is specified, the default profile is used (``sentinelhub.config.DEFAULT_PROFILE``, currently set to ``"default-profile"``). This is also used whenever no explicit ``SHConfig`` is provided to a function/class, unless the preferred profile is set via the `SH_PROFILE` environment variable (more about that in a later section).

The configuration file can be found at ``~/.config/sentinelhub/config.toml``. On Windows this usually translates to ``C:/Users/<USERNAME>/.config/sentinelhub/config.toml``. You can get the precise location of the file by calling ``SHConfig.get_config_location()``.

The configuration file follows the standard TOML structure. Sections are denoted by the profile name in square brackets, while the following lines specify ``key=value`` pairs for any fields that should be updated, for example:

.. code-block:: toml

    [default-profile]
    instance_id = "my-instance-id"
    max_download_attempts = 3

    [custom-profile]
    instance_id = "something-else"

One can also view the settings for a given profile in the CLI with the command ``sentinelhub.config --profile my-profile --show``.

The file can also be updated programmatically by using the ``save`` method.

.. code-block:: python

    from sentinelhub import SHConfig

    config = SHConfig()
    config.instance_id = "my-instance-id"
    config.save("my-profile")


Another option is to update the configuration file via CLI. However this approach can only modify existing profiles, any new profiles need to be added manually or through the Python interface.::

$ sentinelhub.config --profile my-profile --instance_id my-instance-id

Environment Variables
*********************

We generally suggest using the configuration file, but we offer limited support for environmental variables to simplify situations such as building docker images.

The ``SHConfig`` class reads the following environmental variables during initialization:

- ``SH_PROFILE`` that dictates which profile should be used when not explicitly provided.
- ``SH_CLIENT_ID`` and ``SH_CLIENT_SECRET`` for setting the SentinelHub credentials.


Precedence
**********

The general precedence order is ``explicit parameters > environment > configuration file > defaults``.

This means that ``SHConfig(profile="my-profile", sh_client_id="my-id")`` will be taken into account over ``SH_PROFILE`` and ``SH_CLIENT_ID`` environment variables, which would take precedence over what is specified in the ``configuration.toml``.


Sentinel Hub Configuration
**************************


In order to use Sentinel Hub services you will need a Sentinel Hub account. If you do not have one yet, you can
create a free trial account at `Sentinel Hub`_. If you are a researcher you can even apply for a free non-commercial
account through `ESA Network of Resources`_. The following configurations are then linked to your account.

Parameter ``instance_id`` is used when using OGC endpoints of the `Sentinel Hub services`_. It is the identifier of a
configuration users can set up in the `Sentinel Hub Dashboard`_ under "Configuration Utility".

The ``sh_client_id`` and ``sh_client_secret`` parameters can also be created in the `Sentinel Hub Dashboard`_ under
"User settings". The two parameters are needed when accessing protected endpoints of the service (Process, Catalog,
Batch, BYOC, and other APIs). There is "OAuth clients" frame where we can create a new OAuth client.

.. admonition:: Additional information on creating OAuth client

    For detailed instructions on how to obtain credentials, you can see the `Sentinel Hub webinar`_.


Copernicus Data Space Ecosystem Configuration
*********************************************


For Copernicus Data Space Ecosystem users, please follow the `Sentinel Hub Services Authentication instructions`_ to
register an OAuth client using the `Sentinel Hub Services Dashboard`_.

With the registered OAuth client, a valid Copernicus Data Space Ecosystem configuration should be configured as below:

.. code-block:: python

    from sentinelhub import SHConfig

    config = SHConfig()
    config.sh_client_id = 'oauth-client-id'
    config.sh_client_secret = 'oauth-client-secret'
    config.sh_base_url = 'https://sh.dataspace.copernicus.eu'
    config.sh_token_url = 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'


.. admonition:: Supported Data Collections

    With the Copernicus Data Space Ecosystem Configuration, please find the available data collections on
    `Copernicus Data Space Ecosystem indexed in Sentinel Hub`_.


Other configuration options
***************************

For more configuration options check::

$ sentinelhub.config --help


.. _`Sentinel Hub`: https://www.sentinel-hub.com/trial
.. _`ESA Network of Resources`: https://www.sentinel-hub.com/Network-of-Resources/
.. _`Sentinel Hub Dashboard`: https://apps.sentinel-hub.com/dashboard/
.. _`Sentinel Hub services`: https://www.sentinel-hub.com/develop/documentation/api/ogc_api/
.. _`Sentinel Hub webinar`: https://www.youtube.com/watch?v=CBIlTOl2po4&t=1760s
.. _`Sentinel Hub public repository`: https://roda.sentinel-hub.com/sentinel-s2-l1c/
.. _`Sentinel Hub Services Authentication instructions`: https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Overview/Authentication.html
.. _`Sentinel Hub Services Dashboard`: https://shapps.dataspace.copernicus.eu/dashboard/#/
.. _`Copernicus Data Space Ecosystem indexed in Sentinel Hub`: https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Data.html

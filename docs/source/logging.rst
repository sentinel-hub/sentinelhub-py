*************
Logging
*************

The package provides runtime information through the use of logging. To enable basic logs provided by ``sentinelhub`` enable logging on the ``DEBUG`` level.

.. code-block:: python

    import logging

    logging.basicConfig(level=logging.DEBUG)


Because ``sentinelhub`` is using other packages for communication, one can gain more precise information by configuring their logging as well.

.. code-block:: python

    import logging
    from http.client import HTTPConnection

    HTTPConnection.debuglevel = 1

    logging.basicConfig(level=logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


Downloading is multi-threaded with the use of the standard `threading`_ library. This allows the user to also keep track of the thread that logged each message. The simplest way is to configure the logging format to display the name of the thread.

.. code-block:: python

    import logging

    # The default format is '%(levelname)s:%(name)s:%(message)s'

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)s:%(name)s:%(threadName)s:%(message)s',
    )


For more information on advanced logging configuration consult the official `logging documentation`_, which also contains an `advanced logging tutorial`_.


.. _`threading`: https://docs.python.org/3/library/threading.html
.. _`logging documentation`: https://docs.python.org/3/library/logging.html
.. _`advanced logging tutorial`: https://docs.python.org/3/howto/logging.html#logging-advanced-tutorial

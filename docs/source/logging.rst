*******
Logging
*******

The package provides runtime information through the use of logging. To enable basic logs provided by ``sentinelhub`` enable logging on the ``DEBUG`` level.

.. code-block:: python

    import logging

    logging.basicConfig(level=logging.DEBUG)


One can also redirect all warnings that occur during evaluation to the logger.

.. code-block:: python

    import logging

    logging.basicConfig(level=logging.DEBUG)
    logging.captureWarnings(True)


The ``sentinelhub`` package is using ``requests`` package for HTTP communication. In case standard logs are not detailed enough, it is possible to obtain full information about HTTP requests by propagating low-level ``urllib3`` logs.

.. code-block:: python

    import logging
    from http.client import HTTPConnection

    HTTPConnection.debuglevel = 1

    logging.basicConfig(level=logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


Downloading is multi-threaded with the use of the standard `threading`_ library. The simplest way to keep track, which log comes from which thread, is to add thread names into logging formatting.

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

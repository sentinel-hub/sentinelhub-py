************
Installation
************

This package requires Python >=3.6 and can be installed with PyPI package manager::

$ pip install sentinelhub --upgrade

Alternatively, the package can be installed with Conda from `conda-forge` channel::

$ conda install -c conda-forge sentinelhub

In order to install the latest (development) version clone the GitHub_ repository and install::

$ pip install -e . --upgrade

or manually::

$ python setup.py build
$ python setup.py install

Before installing ``sentinelhub-py`` on **Windows** it is recommended to install ``shapely`` package from
Unofficial Windows wheels repository (link_).


.. _Github: https://github.com/sentinel-hub/sentinelhub-py
.. _link: https://www.lfd.uci.edu/~gohlke/pythonlibs/
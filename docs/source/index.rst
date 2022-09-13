bigquery-operator documentation
===============================

.. image:: https://img.shields.io/pypi/v/bigquery-operator
    :target: https://pypi.org/project/bigquery-operator

.. image:: https://img.shields.io/pypi/l/bigquery-operator.svg
    :target: https://pypi.org/project/bigquery-operator

.. image:: https://img.shields.io/pypi/pyversions/bigquery-operator.svg
    :target: https://pypi.org/project/bigquery-operator

.. image:: https://codecov.io/gh/augustin-barillec/bigquery-operator/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/augustin-barillec/bigquery-operator

.. image:: https://pepy.tech/badge/bigquery-operator
    :target: https://pepy.tech/project/bigquery-operator

Wrapper for usual operations on a fixed BigQuery dataset.

Acknowledgements
----------------
I am grateful to my employer Easyence_ for providing me the resources to develop this library and for allowing me
to publish it.

Installation
------------

::

    $ pip install bigquery-operator

Quickstart
----------

Set up a operator.

In the following code, the credentials are inferred from the environment.
For further information about how to authenticate to Google Cloud Platform with the
`Google Cloud Client Library for Python`_, have a look
`here <https://googleapis.dev/python/google-api-core/latest/auth.html>`__.

.. code-block:: python

    >>> from bigquery_operator import OperatorQuickSetup

    >>> o = OperatorQuickSetup(
                project_id='tmp_project',
                dataset_name='tmp_dataset',
                credentials=None)

Execute various operations on the dataset 'tmp_dataset'.

.. code-block:: python



Required packages
-----------------

- google-cloud-bigquery


Table of Contents
-----------------

.. toctree::
   :maxdepth: 3

   API
   history

.. _Easyence: https://www.easyence.com/en/
.. _Google Cloud Client Library for Python: https://github.com/googleapis/google-cloud-python#google-cloud-python-client

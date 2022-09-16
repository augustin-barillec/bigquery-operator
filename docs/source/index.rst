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

Set up an operator.

In the following code, the credentials are inferred from the environment.
For further information about how to authenticate to Google Cloud Platform with the
`Google Cloud Client Library for Python`_, have a look
`here <https://googleapis.dev/python/google-api-core/latest/auth.html>`__.

.. code-block:: python

    project_id = 'tmp_project'
    dataset_name = 'tmp_dataset'

    from bigquery_operator import OperatorQuickSetup
    o = OperatorQuickSetup(
            project_id,
            dataset_name,
            credentials=None)

Execute various operations on the dataset 'tmp_dataset'.

.. code-block:: python

    >>> o.dataset_exists()
    False
    >>> o.create_dataset(
    >>>     location='EU',
    >>>     default_time_to_live=10)
    >>> o.dataset_exists()
    True
    >>> o.list_tables()
    []
    >>> o.table_exists('table_1')
    False
    >>> o.run_query('select 3.14 as x', 'table_1')
    {'duration': 3, 'cost': 0.0}
    >>> o.table_exists('table_1')
    True
    >>> o.run_query('select 3.14 as x', 'table_1')
    {'duration': 2, 'cost': 0.0}
    >>> o.table_exists('table_1')
    True
    >>> o.create_view('select 2.718 as x', 'table_2')
    >>> o.copy_table('table_1', 'copy_table_1')
    # Let suppose the table tmp1_project.tmp1_dataset.table_3 exists.
    # Then one can copy it into tmp_dataset.
    >>> source_dataset_id = 'tmp1_project.tmp1_dataset'
    >>> o.copy_table(
    >>>     'table_3', 'copy_table_3',
    >>>     source_dataset_id=source_dataset_id)
    # Let suppose the bucket tmp_bucket exists.
    # Then one can extract a table from tmp_dataset into tmp_bucket.
    >>> bucket_name = 'tmp_bucket'
    >>> blob_name = 'table_1-*.csv.gz'
    >>> uri = f'gs://{bucket_name}/{blob_name}'
    >>> o.extract_table('table_1', uri, print_header=True)
    # Conversely, one can load data from tmp_bucket into tmp_dataset.
    >>> o.load_table(uri, 'table_10')
    # The methods run_queries, copy_tables, extract_tables and
    # load_tables are run in parallel. For instance:
    >>> queries = [f'select {i}' for i in range(4)]
    >>> destination_table_names = [f'table_4{i}' for i in range(4)]
    >>> o.run_queries(queries, destination_table_names)
    {'duration': 4, 'cost': 0.0}
    >>> o.list_tables()
    ['copy_table_1', 'copy_table_3', 'table_1',
     'table_10', 'table_2', 'table_40', 'table_41',
     'table_42', 'table_43']
    >>> o.delete_table('table_43')
    >>> o.list_tables()
    ['copy_table_1', 'copy_table_3', 'table_1',
     'table_10', 'table_2', 'table_40', 'table_41',
     'table_42']
    >>> from google.cloud import bigquery
    >>> schema = [
    >>>     bigquery.SchemaField('y', 'INTEGER'),
    >>>     bigquery.SchemaField('z', 'TIMESTAMP')]
    >>> time_partitioning = bigquery.TimePartitioning(
    >>>             field='z', type_='DAY')
    >>> o.create_empty_table(
    >>>     table_name='table_5',
    >>>     schema=schema,
    >>>     time_partitioning=time_partitioning)
    >>> o.table_is_empty('table_5')
    True
    >>> query = """
    >>> select 5 as y,
    >>> cast('2012-11-14 14:32:30' as TIMESTAMP) as z
    >>> """
    >>> o.run_query(query, 'table_5')
    {'duration': 3, 'cost': 0.0}
    >>> o.table_is_empty('table_5')
    False
    >>> o.get_table('table_5').num_rows
    1
    >>> o.run_query(query, 'table_5',
    >>>     write_disposition='WRITE_APPEND')
    {'duration': 2, 'cost': 0.0}
    >>> o.get_table('table_5').num_rows
    2
    >>> o.list_tables()
    ['copy_table_1', 'copy_table_3', 'table_1', 'table_10',
     'table_2', 'table_40', 'table_41', 'table_42', 'table_5']
    >>> o.get_columns('table_1')
    ['x']
    >>> o.get_columns('table_5')
    ['y', 'z']
    >>> o.get_format_attributes('table_1')
    {'schema': [
         SchemaField('x', 'FLOAT', 'NULLABLE', None, (), None)],
     'time_partitioning': None,
     'range_partitioning': None,
     'require_partition_filter': None,
     'clustering_fields': None}
    >>> o.get_format_attributes('table_5')
    {'schema': [
         SchemaField('y', 'INTEGER', 'NULLABLE', None, (), None),
         SchemaField('z', 'TIMESTAMP', 'NULLABLE', None, (), None)],
     'time_partitioning': TimePartitioning(field='z',type_='DAY'),
     'range_partitioning': None,
     'require_partition_filter': None,
     'clustering_fields': None}
    >>> for n in o.list_tables():
    >>>     o.set_time_to_live(table_name=n, nb_days=5)
    >>> o.get_table('table_1').expires
    datetime.datetime(2022, 9, 19, 13, 58, 3, 727000,
        tzinfo=datetime.timezone.utc)
    >>> o.clean_dataset()
    >>> o.list_tables()
    []
    >>> o.delete_dataset()
    >>> o.dataset_exists()
    False

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

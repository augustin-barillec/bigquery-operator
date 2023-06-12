.. :changelog:

History
=======
2.0 (2023-06-12)
------------------
API Changes
^^^^^^^^^^^
* pandas==2.* is now required.
* When a table is extracted from BigQuery to Storage, it is not compressed
  anymore by default.

Improvement
^^^^^^^^^^^
* The price of a query is not returned anymore. Instead the number of
  gigabytes processed by the query, which is a simpler metric to compute,
  is now returned.
* The method get_table_rows and get_query_rows have been added.
* The methods create_empty_table and create_view now have a
  pre_delete_if_exists argument.
* The methods create_empty_table, create_view, run_queries, load_tables,
  copy_tables, run_query, load_table and copy_table have now a time_to_live
  argument, which is applied to the created tables.
* The method set_time_to_live has now some retries making it more robust
  to google.api_core.exceptions.PreconditionFailed exceptions.

1.0 (2022-09-15)
----------------
* Initial release on PyPI.

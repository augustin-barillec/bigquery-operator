from google.cloud import bigquery
from tests.utils import ids


def instantiate_table(table_name):
    table_id = ids.build_table_id(table_name)
    return bigquery.Table(table_id)

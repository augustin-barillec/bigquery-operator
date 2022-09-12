from google.cloud import bigquery, exceptions
from tests.utils import constants


def build_table_id(table_name):
    return f'{constants.dataset_id}.{table_name}'


def instantiate_table(table_name):
    table_id = build_table_id(table_name)
    return bigquery.Table(table_id)


def get_table(table_name):
    table_id = build_table_id(table_name)
    return constants.bq_client.get_table(table_id)


def table_exists(table_name):
    try:
        get_table(table_name)
        return True
    except exceptions.NotFound:
        return False


def delete_table(table_name):
    table_id = build_table_id(table_name)
    constants.bq_client.delete_table(table_id, not_found_ok=False)


def create_empty_table(
        table_name,
        schema=None,
        time_partitioning=None,
        range_partitioning=None,
        require_partition_filter=None,
        clustering_fields=None):
    table = instantiate_table(table_name)
    table.schema = schema
    table.time_partitioning = time_partitioning
    table.range_partitioning = range_partitioning
    table.require_partition_filter = require_partition_filter
    table.clustering_fields = clustering_fields
    constants.bq_client.create_table(table, exists_ok=False)

from google.cloud import bigquery
from tests.utils import constants, instantiate


def create_dataset():
    dataset = bigquery.Dataset(constants.dataset_id)
    dataset.location = constants.dataset_location
    constants.bq_client.create_dataset(
        dataset=dataset,
        exists_ok=False)


def create_bucket():
    constants.gs_client.create_bucket(
            bucket_or_name=constants.bucket_name,
            location=constants.bucket_location)


def create_empty_table(
        table_name,
        schema=None,
        time_partitioning=None,
        range_partitioning=None,
        require_partition_filter=None,
        clustering_fields=None):
    table = instantiate.instantiate_table(table_name)
    table.schema = schema
    table.time_partitioning = time_partitioning
    table.range_partitioning = range_partitioning
    table.require_partition_filter = require_partition_filter
    table.clustering_fields = clustering_fields
    constants.bq_client.create_table(table, exists_ok=False)

from google.cloud import bigquery, exceptions
from tests.utils import constants


def get_dataset():
    return constants.bq_client.get_dataset(constants.dataset_id)


def dataset_exists():
    try:
        get_dataset()
        return True
    except exceptions.NotFound:
        return False


def delete_dataset():
    constants.bq_client.delete_dataset(
        constants.dataset_id,
        delete_contents=True,
        not_found_ok=True)


def create_dataset():
    dataset = bigquery.Dataset(constants.dataset_id)
    dataset.location = constants.dataset_location
    constants.bq_client.create_dataset(
        dataset=dataset,
        exists_ok=False)


def list_tables():
    tables = list(constants.bq_client.list_tables(constants.dataset_id))
    table_names = sorted([t.table_id for t in tables])
    return table_names

from tests.utils import constants


def build_table_id(table_name):
    return f'{constants.dataset_id}.{table_name}'


def build_bucket_uri(blob_name):
    return f'gs://{constants.bucket_name}/{blob_name}'

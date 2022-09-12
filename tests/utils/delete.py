from tests.utils import constants, ids


def delete_dataset():
    constants.bq_client.delete_dataset(
        constants.dataset_id,
        delete_contents=False,
        not_found_ok=False)


def delete_table(table_name):
    table_id = ids.build_table_id(table_name)
    constants.bq_client.delete_table(table_id, not_found_ok=False)


def delete_bucket():
    constants.bucket.delete()

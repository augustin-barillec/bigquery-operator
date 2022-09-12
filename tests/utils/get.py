from tests.utils import constants, ids


def get_dataset():
    return constants.bq_client.get_dataset(constants.dataset_id)


def get_table(table_name):
    table_id = ids.build_table_id(table_name)
    return constants.bq_client.get_table(table_id)

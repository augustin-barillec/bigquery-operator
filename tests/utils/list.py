from tests.utils import constants


def list_table_names():
    return sorted(
        [t.table_id for t in
         constants.bq_client.list_tables(constants.dataset_id)])


def list_blobs():
    return list(constants.bucket.list_blobs())

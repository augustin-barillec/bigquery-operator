from tests.utils import constants, delete, list


def empty_dataset():
    for n in list.list_table_names():
        delete.delete_table(n)


def empty_bucket():
    blobs = list.list_blobs()
    constants.bucket.delete_blobs(blobs=blobs)

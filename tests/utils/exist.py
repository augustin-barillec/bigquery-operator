from google.cloud import exceptions
from tests.utils import constants, get


def dataset_exists():
    try:
        get.get_dataset()
        return True
    except exceptions.NotFound:
        return False


def table_exists(table_name):
    try:
        get.get_table(table_name)
        return True
    except exceptions.NotFound:
        return False


def bucket_exists():
    return constants.bucket.exists()

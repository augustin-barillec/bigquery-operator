from tests.utils import constants


def build_bucket_uri(blob_name):
    return f'gs://{constants.bucket_name}/{blob_name}'


def bucket_exists():
    return constants.bucket.exists()


def create_bucket():
    constants.gs_client.create_bucket(
            bucket_or_name=constants.bucket_name,
            location=constants.bucket_location)


def list_blobs():
    return list(constants.bucket.list_blobs())


def empty_bucket():
    blobs = list_blobs()
    constants.bucket.delete_blobs(blobs=blobs)


def delete_bucket():
    if bucket_exists():
        empty_bucket()
    constants.bucket.delete()

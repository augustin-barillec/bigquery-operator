import zlib
import pandas
from io import StringIO
from google.cloud import bigquery, storage
from tests.utils import constants, table


def query_to_dataset(query, destination_table_name):
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table.build_table_id(destination_table_name)
    job_config.write_disposition = 'WRITE_TRUNCATE'
    constants.bq_client.query(query=query, job_config=job_config).result()


def dataset_to_bucket(table_name, destination_uri):
    table_id = table.build_table_id(table_name)
    job_config = bigquery.ExtractJobConfig()
    job_config.field_delimiter = constants.field_delimiter
    job_config.destination_format = 'CSV'
    job_config.compression = 'GZIP'
    job = constants.bq_client.extract_table(
        source=table_id,
        destination_uris=destination_uri,
        job_config=job_config)
    job.result()


def dataset_to_dataframe(table_name):
    table_id = table.build_table_id(table_name)
    return constants.bq_client.list_rows(table_id).to_dataframe()


def bucket_to_dataframe(blob_name):
    b = storage.Blob(
        name=blob_name, bucket=constants.bucket).download_as_bytes()
    b = zlib.decompress(b, wbits=zlib.MAX_WBITS | 16)
    csv = b.decode('utf-8')
    return pandas.read_csv(StringIO(csv), sep=constants.field_delimiter)


def dataframe_to_bucket(dataframe, blob_name):
    csv = dataframe.to_csv(sep=constants.field_delimiter, index=False)
    storage.Blob(name=blob_name, bucket=constants.bucket).upload_from_string(
        csv)

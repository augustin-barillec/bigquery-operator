from typing import List, Optional
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery, exceptions


class Operator:
    """Wrapper for usual operations on a fixed BigQuery dataset.

    Args:
        client (google.cloud.bigquery.client.Client): Client to
            manage connections to the BigQuery API.
        dataset_id (str): The dataset id in the format
            'project_id.dataset_name'.
    """

    def __init__(
            self,
            client: bigquery.Client,
            dataset_id: str) -> None:
        self._client = client
        self._dataset_id = dataset_id
        self._dataset_name = self._dataset_id.split('.')[-1]

    @property
    def client(self) -> bigquery.Client:
        """google.cloud.bigquery.client.Client: The client."""
        return self._client

    @property
    def project_id(self) -> str:
        """str: the id of the project which the client acts on behalf of."""
        return self._client.project

    @property
    def dataset_id(self) -> str:
        """str: The dataset id in the format 'project_id.dataset_name'."""
        return self._dataset_id

    @property
    def dataset_name(self) -> str:
        """str: The dataset name."""
        return self._dataset_name

    @staticmethod
    def _wait_for_jobs(jobs: List[bigquery.UnknownJob]) -> None:
        for job in jobs:
            job.result()

    @staticmethod
    def sample_query(query: str, size: int) -> str:
        """Sample randomly a query.

        The output query gives a subset of the lines given by the input query.
        This subset has approximately ``size`` lines. Nonetheless, the cost of
        the output query is the same as the cost of the input query.
        """
        return (f'select * from ({query}) '
                f'where rand() < {size}/(select count(*) from ({query}))')

    def instantiate_dataset(self) -> bigquery.Dataset:
        """Instantiate the dataset. No api call is made."""
        return bigquery.Dataset(self._dataset_id)

    def get_dataset(self) -> bigquery.Dataset:
        """Get the dataset. An api call is made."""
        return self._client.get_dataset(self._dataset_id)

    def dataset_exists(self) -> bool:
        """Return True if the dataset exists."""
        try:
            self.get_dataset()
            return True
        except exceptions.NotFound:
            return False

    def delete_dataset(self) -> None:
        """Delete the dataset."""
        self._client.delete_dataset(
            self._dataset_id,
            delete_contents=False,
            not_found_ok=False)

    def create_dataset(
            self,
            location: Optional[str] = None,
            default_time_to_live: Optional[int] = None) -> None:
        """Create the dataset.

        Args:
            location (str, optional): Location in which the dataset is hosted.
            default_time_to_live (int, optional): The default time to live
                in days for the tables created in the dataset.
        """
        dataset = self.instantiate_dataset()
        dataset.location = location
        if default_time_to_live is not None:
            default_table_expiration_ms = \
                default_time_to_live * 24 * 60 * 60 * 1000
        else:
            default_table_expiration_ms = None
        dataset.default_table_expiration_ms = default_table_expiration_ms
        self._client.create_dataset(dataset=dataset, exists_ok=False)

    def list_tables(self) -> List[str]:
        """List the names of the tables in the dataset."""
        tables = list(self._client.list_tables(self._dataset_id))
        table_names = sorted([t.table_id for t in tables])
        return table_names

    def clean_dataset(self) -> None:
        """Delete all the tables from the dataset."""
        for table_name in self.list_tables():
            self.delete_table(table_name)

    def build_table_id(
            self, table_name: str, dataset_id: Optional[str] = None):
        """Return a table id.

        Args:
            table_name (str): A table name.
            dataset_id (str, optional): A dataset id in the format
                'project_id.dataset_name'. If not passed, falls back to
                self.dataset_id.
        Returns:
            str: A table id in the format 'project_id.dataset_name.table_name'.
        """
        if dataset_id is None:
            dataset_id = self._dataset_id
        return f'{dataset_id}.{table_name}'

    def instantiate_table(self, table_name: str) -> bigquery.Table:
        """Instantiate a table. No api call is made."""
        table_id = self.build_table_id(table_name)
        return bigquery.Table(table_id)

    def get_table(self, table_name: str) -> bigquery.Table:
        """Get a table. An api call is made."""
        table_id = self.build_table_id(table_name)
        return self._client.get_table(table_id)

    def table_exists(self, table_name: str) -> bool:
        """Return True if the table exists."""
        try:
            self.get_table(table_name)
            return True
        except exceptions.NotFound:
            return False

    def delete_table(self, table_name: str) -> None:
        """Delete a table."""
        table_id = self.build_table_id(table_name)
        self._client.delete_table(table_id, not_found_ok=False)

    def create_empty_table(
            self,
            table_name: str,
            schema: Optional[List[bigquery.SchemaField]] = None,
            time_partitioning: Optional[bigquery.TimePartitioning] = None,
            range_partitioning: Optional[bigquery.RangePartitioning] = None,
            require_partition_filter: Optional[bool] = None,
            clustering_fields: Optional[List[str]] = None) -> None:
        """Create a empty table. Only specify at
        most one of time_partitioning or range_partitioning.
        """
        table = self.instantiate_table(table_name)
        table.schema = schema
        table.time_partitioning = time_partitioning
        table.range_partitioning = range_partitioning
        table.require_partition_filter = require_partition_filter
        table.clustering_fields = clustering_fields
        self._client.create_table(table, exists_ok=False)

    def table_is_empty(self, table_name: str) -> bool:
        """Return True if the table is empty."""
        num_rows = self.get_table(table_name).num_rows
        return num_rows == 0

    def get_columns(self, table_name: str) -> List[str]:
        """Return the column names of a table."""
        schema = self.get_table(table_name).schema
        return [f.name for f in schema]

    def get_format_attributes(self, table_name):
        """Return the following table attributes:
        schema, time_partitioning, range_partitioning,
        require_partition_filter, clustering_fields.
        """
        res = dict()
        for a in ['schema', 'time_partitioning', 'range_partitioning',
                  'require_partition_filter', 'clustering_fields']:
            res[a] = getattr(self.get_table(table_name), a)
        return res

    def set_time_to_live(self, table_name: str, nb_days: int) -> None:
        """Set the time to live of a table in days."""
        expiration_time = (datetime.now(timezone.utc) +
                           timedelta(days=nb_days))
        table = self.get_table(table_name)
        table.expires = expiration_time
        self._client.update_table(table, ['expires'])

    def create_view(
            self,
            query: str,
            destination_table_name: str) -> None:
        """Create a view."""
        view = self.instantiate_table(destination_table_name)
        view.view_query = query
        self._client.create_table(view, exists_ok=False)

    def _query_job(
            self,
            query: str,
            destination_table_name: str,
            write_disposition: bigquery.WriteDisposition
    ) -> bigquery.QueryJob:
        job_config = bigquery.QueryJobConfig()
        job_config.destination = self.build_table_id(destination_table_name)
        job_config.write_disposition = write_disposition
        job = self._client.query(query=query, job_config=job_config)
        return job

    def _extract_job(
            self,
            source_table_name: str,
            destination_uri: str,
            field_delimiter: str,
            print_header: bool
    ) -> bigquery.ExtractJob:
        if not destination_uri.endswith('.csv.gz'):
            raise ValueError("destination_uri must end with '.csv.gz'")
        source = self.build_table_id(source_table_name)
        job_config = bigquery.ExtractJobConfig()
        job_config.field_delimiter = field_delimiter
        job_config.print_header = print_header
        job_config.destination_format = 'CSV'
        job_config.compression = 'GZIP'
        job = self._client.extract_table(
            source=source,
            destination_uris=destination_uri,
            job_config=job_config)
        return job

    def _load_job(
            self,
            source_uri: str,
            destination_table_name: str,
            schema: List[bigquery.SchemaField],
            field_delimiter: str,
            write_disposition: bigquery.WriteDisposition
    ) -> bigquery.LoadJob:
        destination = self.build_table_id(destination_table_name)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = 'CSV'
        job_config.field_delimiter = field_delimiter
        if schema is None:
            job_config.autodetect = True
        else:
            job_config.schema = schema
            job_config.skip_leading_rows = 1
        job_config.write_disposition = write_disposition
        job = self._client.load_table_from_uri(
            source_uris=source_uri,
            destination=destination,
            job_config=job_config)
        return job

    def _copy_job(
            self,
            source_table_name: str,
            destination_table_name: str,
            source_dataset_id: str,
            write_disposition: bigquery.WriteDisposition
    ) -> bigquery.CopyJob:
        source_table_id = self.build_table_id(
            source_table_name, dataset_id=source_dataset_id)
        destination_table_id = self.build_table_id(
            destination_table_name)
        job_config = bigquery.CopyJobConfig()
        job_config.write_disposition = write_disposition
        job = self._client.copy_table(
            sources=source_table_id,
            destination=destination_table_id,
            job_config=job_config)
        return job

    def _query_jobs(
            self,
            queries: List[str],
            destination_table_names: List[str],
            write_disposition: bigquery.WriteDisposition
    ) -> List[bigquery.QueryJob]:
        len_queries = len(queries)
        len_destination_table_names = len(destination_table_names)
        if len_queries == 0:
            raise ValueError('queries must not be empty')
        if len_queries != len_destination_table_names:
            raise ValueError('queries and destination_table_names must have '
                             'the same length')
        return [self._query_job(q, d, write_disposition)
                for q, d in zip(queries, destination_table_names)]

    def _extract_jobs(
            self,
            source_table_names: List[str],
            destination_uris: List[str],
            field_delimiter: str,
            print_header: bool
    ) -> List[bigquery.ExtractJob]:
        len_source_table_names = len(source_table_names)
        len_destination_uris = len(destination_uris)
        if len_source_table_names == 0:
            raise ValueError('source_table_names must not be empty')
        if len_source_table_names != len_destination_uris:
            raise ValueError('source_table_names and destination_uris '
                             'must have the same length')
        return [self._extract_job(s, d, field_delimiter, print_header)
                for s, d in zip(source_table_names, destination_uris)]

    def _load_jobs(
            self,
            source_uris: List[str],
            destination_table_names: List[str],
            schemas: List[List[bigquery.SchemaField]],
            field_delimiter: str,
            write_disposition: bigquery.WriteDisposition
    ) -> List[bigquery.LoadJob]:
        len_source_uris = len(source_uris)
        len_destination_table_names = len(destination_table_names)
        if len_source_uris == 0:
            raise ValueError('source_uris must not be empty')
        if len_source_uris != len_destination_table_names:
            raise ValueError('source_uris and destination_table_names '
                             'must have the same length')
        return [
            self._load_job(s, d, sch, field_delimiter, write_disposition)
            for s, d, sch in
            zip(source_uris, destination_table_names, schemas)]

    def _copy_jobs(
            self,
            source_table_names: List[str],
            destination_table_names: List[str],
            source_dataset_id: str,
            write_disposition: bigquery.WriteDisposition
    ) -> List[bigquery.CopyJob]:
        len_source_table_names = len(source_table_names)
        len_destination_table_names = len(destination_table_names)
        if len_source_table_names == 0:
            raise ValueError('source_table_names must not be empty')
        if len_source_table_names != len_destination_table_names:
            raise ValueError('source_table_names and destination_table_names '
                             'must have the same length')
        return [
            self._copy_job(s, d, source_dataset_id, write_disposition)
            for s, d in zip(source_table_names, destination_table_names)]

    def run_queries(
            self,
            queries: List[str],
            destination_table_names: List[str],
            write_disposition: Optional[bigquery.WriteDisposition] =
            bigquery.WriteDisposition.WRITE_TRUNCATE) -> dict:
        """Run queries. Return monitoring as a dict in the format
        {'duration': d, 'cost': c} where d is the execution duration in
        seconds and c the execution cost in dollars.
        """
        start_timestamp = datetime.now(timezone.utc)
        jobs = self._query_jobs(
            queries, destination_table_names, write_disposition)
        self._wait_for_jobs(jobs)
        end_timestamp = datetime.now(timezone.utc)
        duration = round((end_timestamp - start_timestamp).total_seconds())
        total_bytes_billed_list = [j.total_bytes_billed for j in jobs]
        costs = [round(tbb / 10 ** 12 * 5, 5)
                 for tbb in total_bytes_billed_list]
        cost = sum(costs)
        monitoring = {'duration': duration, 'cost': cost}
        return monitoring

    def extract_tables(
            self,
            source_table_names: List[str],
            destination_uris: List[str],
            field_delimiter: Optional[str] = '|',
            print_header: Optional[bool] = True) -> None:
        """Extract tables from BigQuery to Storage. Each source table is
        extracted as one or more compressed gzip csv files. Each destination
        uri must end with '.csv.gz'.
        """
        self._wait_for_jobs(self._extract_jobs(
            source_table_names,
            destination_uris,
            field_delimiter,
            print_header))

    def load_tables(
            self,
            source_uris: List[str],
            destination_table_names: List[str],
            schemas: Optional[List[List[bigquery.SchemaField]]] = None,
            field_delimiter: Optional[str] = '|',
            write_disposition: Optional[bigquery.WriteDisposition] =
            bigquery.WriteDisposition.WRITE_TRUNCATE) -> None:
        """Load Storage CSV files into BigQuery tables."""
        if schemas is None:
            schemas = [None]*len(source_uris)
        self._wait_for_jobs(self._load_jobs(
            source_uris, destination_table_names, schemas,
            field_delimiter, write_disposition))

    def copy_tables(
            self,
            source_table_names: List[str],
            destination_table_names: List[str],
            source_dataset_id: Optional[str] = None,
            write_disposition: Optional[bigquery.WriteDisposition] =
            bigquery.WriteDisposition.WRITE_TRUNCATE) -> None:
        """Copy tables. ``source_dataset_id`` must be given in the format
        'project_id.dataset_name'. If not passed, falls back to
        self.dataset_id.
        """
        if source_dataset_id is None:
            source_dataset_id = self._dataset_id
        self._wait_for_jobs(self._copy_jobs(
            source_table_names, destination_table_names,
            source_dataset_id, write_disposition))

    def run_query(
            self,
            query: str,
            destination_table_name: str,
            write_disposition: Optional[bigquery.WriteDisposition] =
            bigquery.WriteDisposition.WRITE_TRUNCATE) -> dict:
        """Run a query. Return monitoring as a dict in the format
        {'duration': d, 'cost': c} where d is the execution duration in
        seconds and c the execution cost in dollars.
        """
        return self.run_queries(
            [query], [destination_table_name], write_disposition)

    def extract_table(
            self,
            source_table_name: str,
            destination_uri: str,
            field_delimiter: Optional[str] = '|',
            print_header: Optional[bool] = True) -> None:
        """Extract a table. Data is extracted as a gzip compressed csv.
        ``destination_uri`` must end with '.csv.gz'
        """
        self.extract_tables(
            [source_table_name], [destination_uri],
            field_delimiter, print_header)

    def load_table(
            self,
            source_uri: str,
            destination_table_name: str,
            schema: Optional[List[bigquery.SchemaField]] = None,
            field_delimiter: Optional[str] = '|',
            write_disposition: Optional[bigquery.WriteDisposition] =
            bigquery.WriteDisposition.WRITE_TRUNCATE) -> None:
        """Load one or more Storage CSV files into one BigQuery table."""
        self.load_tables(
            [source_uri], [destination_table_name], [schema],
            field_delimiter, write_disposition)

    def copy_table(
            self,
            source_table_name: str,
            destination_table_name: str,
            source_dataset_id: Optional[str] = None,
            write_disposition: Optional[bigquery.WriteDisposition] =
            bigquery.WriteDisposition.WRITE_TRUNCATE) -> None:
        """Copy a table. ``source_dataset_id`` must be given in the format
        'project_id.dataset_name'. If not passed, falls back to
        self.dataset_id.
        """
        self.copy_tables(
            [source_table_name], [destination_table_name],
            source_dataset_id, write_disposition)

import pandas
from google.cloud import bigquery
from tests import utils as ut


class JobsTest(ut.base_class.BaseClassTest):

    def test_run_queries(self):
        expected_1 = pandas.DataFrame(
            data={'x': [3], 'y': ['a']})
        expected_2 = pandas.DataFrame(data={'x': [1]})

        query_1 = "select 3 as x, 'a' as y"
        query_2 = "select 1 as x"
        monitoring = ut.operators.operator.run_queries(
            queries=[query_1, query_2],
            destination_table_names=['table_name_1', 'table_name_2'])

        self.assertEqual(['cost', 'duration'], sorted(monitoring.keys()))
        self.assertTrue(isinstance(monitoring['cost'], float))
        self.assertEqual(0.0, monitoring['cost'])
        self.assertTrue(isinstance(monitoring['duration'], int))
        self.assertTrue(monitoring['duration'] > 0)
        self.assertEqual(
            ['table_name_1', 'table_name_2'], ut.list.list_table_names())

        computed_1 = ut.load.dataset_to_dataframe('table_name_1')
        computed_2 = ut.load.dataset_to_dataframe('table_name_2')
        ut.pandas_equal.assert_equal(expected_1, computed_1)
        ut.pandas_equal.assert_equal(expected_2, computed_2)

    def test_extract_tables(self):
        expected_1 = pandas.DataFrame(
            data={'x': [3], 'y': ['a']})
        expected_2 = pandas.DataFrame(data={'x': [1]})

        query_1 = "select 3 as x, 'a' as y"
        query_2 = "select 1 as x"
        ut.load.query_to_dataset(query_1, 'table_name_1')
        ut.load.query_to_dataset(query_2, 'table_name_2')

        if ut.exist.bucket_exists():
            ut.empty.empty_bucket()
            ut.delete.delete_bucket()
        ut.create.create_bucket()

        blob_name_1 = 'tmp/table_name_1.csv.gz'
        blob_name_2 = 'tmp/table_name_2.csv.gz'

        uri_1 = f'gs://{ut.constants.bucket_name}/{blob_name_1}'
        uri_2 = f'gs://{ut.constants.bucket_name}/{blob_name_2}'

        ut.operators.operator.extract_tables(
            source_table_names=['table_name_1', 'table_name_2'],
            destination_uris=[uri_1, uri_2])

        computed_1 = ut.load.bucket_to_dataframe(blob_name_1, True)
        computed_2 = ut.load.bucket_to_dataframe(blob_name_2, True)

        self.assert_pandas_equal(expected_1, computed_1)
        self.assert_pandas_equal(expected_2, computed_2)

        ut.empty.empty_bucket()
        ut.delete.delete_bucket()

    def test_load_tables(self):
        expected_1 = pandas.DataFrame(
            data={'x': [3], 'y': ['a']})
        expected_2 = pandas.DataFrame(data={'x': [1]})

        if ut.exist.bucket_exists():
            ut.empty.empty_bucket()
            ut.delete.delete_bucket()
        ut.create.create_bucket()

        blob_name_1 = 'tmp/table_name_1.csv'
        blob_name_2 = 'tmp/table_name_2.csv'

        ut.load.dataframe_to_bucket(expected_1, blob_name_1)
        ut.load.dataframe_to_bucket(expected_2, blob_name_2)

        source_uri_1 = f'gs://{ut.constants.bucket_name}/{blob_name_1}'
        source_uri_2 = f'gs://{ut.constants.bucket_name}/{blob_name_2}'

        ut.operators.operator.load_tables(
            source_uris=[source_uri_1, source_uri_2],
            destination_table_names=['table_name_1', 'table_name_2'])

        computed_1 = ut.load.dataset_to_dataframe('table_name_1')
        computed_2 = ut.load.dataset_to_dataframe('table_name_2')
        self.assert_pandas_equal(expected_1, computed_1)
        self.assert_pandas_equal(expected_2, computed_2)

        ut.empty.empty_bucket()
        ut.delete.delete_bucket()

    def test_copy_tables(self):
        expected_1 = pandas.DataFrame(
            data={'x': [3], 'y': ['a']})
        expected_2 = pandas.DataFrame(data={'x': [1]})

        query_1 = "select 3 as x, 'a' as y"
        query_2 = "select 1 as x"
        ut.load.query_to_dataset(query_1, 'table_name_1')
        ut.load.query_to_dataset(query_2, 'table_name_2')

        ut.operators.operator.copy_tables(
            source_table_names=['table_name_1', 'table_name_2'],
            destination_table_names=['copy_table_name_1', 'copy_table_name_2'])

        computed_1 = ut.load.dataset_to_dataframe('copy_table_name_1')
        computed_2 = ut.load.dataset_to_dataframe('copy_table_name_2')
        self.assert_pandas_equal(expected_1, computed_1)
        self.assert_pandas_equal(expected_2, computed_2)

    def test_run_query(self):
        expected = pandas.DataFrame(
            data={'x': [3, 2], 'y': ['a', 'b']})

        query = """
        select 3 as x, 'a' as y union all select 2 as x, 'b' as y
        """
        monitoring = ut.operators.operator.run_query(
            query=query,
            destination_table_name='table_name')
        self.assertEqual(['cost', 'duration'], sorted(monitoring.keys()))
        self.assertTrue(isinstance(monitoring['cost'], float))
        self.assertEqual(0.0, monitoring['cost'])
        self.assertTrue(isinstance(monitoring['duration'], int))
        self.assertTrue(monitoring['duration'] > 0)
        self.assertEqual(['table_name'], ut.list.list_table_names())

        computed = ut.load.dataset_to_dataframe('table_name')
        ut.pandas_equal.assert_equal(expected, computed)

    def test_extract_table(self):
        expected = pandas.DataFrame(
            data={'x': [3, 2], 'y': ['a', 'b']})
        query = """
                select 3 as x, 'a' as y union all select 2 as x, 'b' as y
                """
        ut.load.query_to_dataset(query, 'table_name')
        if ut.exist.bucket_exists():
            ut.empty.empty_bucket()
            ut.delete.delete_bucket()
        ut.create.create_bucket()
        blob_name = 'table_name-*.csv.gz'
        uri = f'gs://{ut.constants.bucket_name}/{blob_name}'
        ut.operators.operator.extract_table(
            source_table_name='table_name',
            destination_uri=uri)
        computed = ut.load.bucket_to_dataframe(
            'table_name-000000000000.csv.gz', True)
        self.assert_pandas_equal(expected, computed)
        ut.empty.empty_bucket()
        ut.delete.delete_bucket()

    def test_load_table(self):
        expected = pandas.DataFrame(
            data={'x': [3, 2], 'y': ['a', 'b']})

        query = """
        select 3 as x, 'a' as y union all select 2 as x, 'b' as y
        """
        ut.load.query_to_dataset(
            query=query,
            destination_table_name='table_name')

        if ut.exist.bucket_exists():
            ut.empty.empty_bucket()
            ut.delete.delete_bucket()
        ut.create.create_bucket()

        uri = f'gs://{ut.constants.bucket_name}/table_name-*.csv.gz'
        ut.load.dataset_to_bucket('table_name', uri)

        ut.operators.operator.load_table(
            source_uri=uri,
            destination_table_name='table_name_bis')
        computed = ut.load.dataset_to_dataframe('table_name_bis')
        self.assert_pandas_equal(expected, computed)

        ut.empty.empty_bucket()
        ut.delete.delete_bucket()

    def test_copy_table(self):
        expected = pandas.DataFrame(
            data={'x': [3, 2], 'y': ['a', 'b']})
        query = """
        select 3 as x, 'a' as y union all select 2 as x, 'b' as y
        """
        source_dataset_id = \
            f'{ut.constants.project_id}.{ut.constants.dataset_name}_1'
        source_dataset = bigquery.Dataset(source_dataset_id)
        source_dataset.location = ut.constants.dataset_location
        ut.constants.bq_client.delete_dataset(
            source_dataset_id, delete_contents=True, not_found_ok=True)
        ut.constants.bq_client.create_dataset(source_dataset_id)
        ut.load.query_to_dataset(query, 'table_name')
        ut.operators.operator.copy_table(
            source_table_name='table_name',
            destination_table_name='copy_table_name',
            source_dataset_id=source_dataset_id)
        computed = ut.load.dataset_to_dataframe('copy_table_name')
        self.assert_pandas_equal(expected, computed)
        ut.constants.bq_client.delete_dataset(
            source_dataset_id, delete_contents=True, not_found_ok=True)

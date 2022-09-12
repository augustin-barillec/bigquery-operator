import unittest
from google.cloud import bigquery
from tests import utils as ut


class TableMethodsWithoutApiCallsTest(unittest.TestCase):

    def test_build_table_id(self):
        expected = ut.ids.build_table_id('table_name')
        computed = ut.operators.operator.build_table_id('table_name')
        self.assertTrue(expected, computed)

    def test_instantiate_table(self):
        expected = ut.instantiate.instantiate_table('table_name')
        computed = ut.operators.operator.instantiate_table('table_name')
        self.assertEqual(expected, computed)


class TableMethodsWithApiCallsTest(ut.base_class.BaseClassTest):

    def test_get_table(self):
        ut.create.create_empty_table('table_name')
        expected = ut.get.get_table('table_name')
        computed = ut.operators.operator.get_table('table_name')
        self.assertEqual(expected, computed)

    def test_table_exists(self):
        self.assertFalse(ut.operators.operator.table_exists('table_name'))
        ut.create.create_empty_table('table_name')
        self.assertTrue(ut.operators.operator.table_exists('table_name'))

    def test_table_is_empty(self):
        ut.create.create_empty_table('table_name')
        self.assertTrue(ut.operators.operator.table_is_empty('table_name'))
        ut.load.query_to_dataset('select 3', 'table_name')
        self.assertFalse(ut.operators.operator.table_is_empty('table_name'))

    def test_list_tables(self):
        self.assertEqual([], ut.operators.operator.list_tables())
        ut.create.create_empty_table('table_name_1')
        ut.create.create_empty_table('table_name_2')
        self.assertEqual(
            ['table_name_1', 'table_name_2'],
            ut.operators.operator.list_tables())

    def test_delete_table(self):
        ut.create.create_empty_table('table_name')
        ut.operators.operator.delete_table('table_name')
        self.assertFalse(ut.exist.table_exists('table_name'))

    def test_create_empty_table(self):
        schema = [
            bigquery.SchemaField('a', 'STRING'),
            bigquery.SchemaField('b', 'TIMESTAMP'),
            bigquery.SchemaField('c', 'INTEGER')]
        time_partitioning = None
        range_partitioning = bigquery.RangePartitioning(
            field='c',
            range_=bigquery.PartitionRange(
                start=0, end=100, interval=10))
        require_partition_filter = True
        clustering_fields = ['a', 'b']
        ut.operators.operator.create_empty_table(
            table_name='table_name_1',
            schema=schema,
            time_partitioning=time_partitioning,
            range_partitioning=range_partitioning,
            require_partition_filter=require_partition_filter,
            clustering_fields=clustering_fields)
        self.assertTrue(ut.exist.table_exists('table_name_1'))
        table = ut.get.get_table('table_name_1')
        self.assertEqual(0, table.num_rows)
        self.assertEqual(schema, table.schema)
        self.assertEqual(time_partitioning, table.time_partitioning)
        self.assertEqual(range_partitioning, table.range_partitioning)
        self.assertEqual(
            require_partition_filter, table.require_partition_filter)
        self.assertEqual(clustering_fields, table.clustering_fields)

        ut.operators.operator.create_empty_table('table_name_2')
        self.assertTrue(ut.exist.table_exists('table_name_2'))
        table = ut.get.get_table('table_name_2')
        self.assertEqual(0, table.num_rows)
        self.assertEqual([], table.schema)
        self.assertEqual(None, table.time_partitioning)
        self.assertEqual(None, table.range_partitioning)
        self.assertEqual(None, table.require_partition_filter)
        self.assertEqual(None, table.clustering_fields)

    def test_get_columns(self):
        expected = []
        ut.create.create_empty_table('table_name_1')
        computed = ut.operators.operator.get_columns('table_name_1')
        self.assertEqual(expected, computed)

        expected = ['a', 'b']
        ut.load.query_to_dataset('select 3 as a, 1 as b', 'table_name_2')
        computed = ut.operators.operator.get_columns('table_name_2')
        self.assertEqual(expected, computed)

    def test_get_format_attributes(self):
        schema = [
            bigquery.SchemaField('a', 'STRING'),
            bigquery.SchemaField('b', 'TIMESTAMP'),
            bigquery.SchemaField('c', 'INTEGER')]
        time_partitioning = bigquery.TimePartitioning(
            field='b', type_='DAY', require_partition_filter=True)
        range_partitioning = None
        require_partition_filter = True
        clustering_fields = ['a', 'b']
        expected = {
            'schema': schema,
            'time_partitioning': time_partitioning,
            'range_partitioning': range_partitioning,
            'require_partition_filter': require_partition_filter,
            'clustering_fields': clustering_fields}
        ut.create.create_empty_table(
            table_name='table_name_1',
            schema=schema,
            time_partitioning=time_partitioning,
            range_partitioning=range_partitioning,
            require_partition_filter=require_partition_filter,
            clustering_fields=clustering_fields)
        query = """
        select 
        'a' as a,
        cast('2022-08-30 22:22:39.728000 UTC' as timestamp) as b,
        3 as c
        """
        ut.load.query_to_dataset(
            query=query,
            destination_table_name='table_name_1')
        table = ut.get.get_table('table_name_1')
        computed = {
            'schema': table.schema,
            'time_partitioning': table.time_partitioning,
            'range_partitioning': table.range_partitioning,
            'require_partition_filter': table.require_partition_filter,
            'clustering_fields': table.clustering_fields}
        self.assertEqual(expected, computed)

        expected = {
            'schema': ['x'],
            'time_partitioning': None,
            'range_partitioning': None,
            'require_partition_filter': None,
            'clustering_fields': None}
        ut.load.query_to_dataset('select 3 as x', 'table_name_2')
        table = ut.get.get_table('table_name_2')
        computed = {
            'schema': table.schema,
            'time_partitioning': table.time_partitioning,
            'range_partitioning': table.range_partitioning,
            'require_partition_filter': table.require_partition_filter,
            'clustering_fields': table.clustering_fields}
        self.assertEqual(expected, computed)

    def test_set_time_to_live(self):
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        approximate_expected = now + timedelta(days=3)
        ut.create.create_empty_table('table_name')
        ut.operators.operator.set_time_to_live('table_name', 3)
        computed = ut.get.get_table('table_name').expires
        delta = (computed - approximate_expected).total_seconds()
        self.assertTrue(0 < delta < 1)

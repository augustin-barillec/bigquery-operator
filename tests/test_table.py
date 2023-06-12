import unittest
from google.cloud import bigquery
from tests import utils as ut


class TableWithoutApiCallsTest(unittest.TestCase):
    def test_build_table_id(self):
        expected = ut.table.build_table_id('table_name')
        computed = ut.operators.operator.build_table_id('table_name')
        self.assertTrue(expected, computed)

    def test_instantiate_table(self):
        expected = ut.table.instantiate_table('table_name')
        computed = ut.operators.operator.instantiate_table('table_name')
        self.assertEqual(expected, computed)


class TableWithApiCallsTest(ut.base_class.BaseClassTest):
    def test_get_table(self):
        ut.table.create_empty_table('table_name')
        expected = ut.table.get_table('table_name')
        computed = ut.operators.operator.get_table('table_name')
        self.assertEqual(expected, computed)

    def test_table_exists(self):
        self.assertFalse(ut.operators.operator.table_exists('table_name'))
        ut.table.create_empty_table('table_name')
        self.assertTrue(ut.operators.operator.table_exists('table_name'))

    def test_delete_table(self):
        ut.table.create_empty_table('table_name')
        ut.operators.operator.delete_table('table_name')
        self.assertFalse(ut.table.table_exists('table_name'))

    def test_delete_table_if_exists(self):
        ut.table.create_empty_table('table_name')
        ut.operators.operator.delete_table_if_exists('table_name')
        self.assertFalse(ut.table.table_exists('table_name'))
        ut.operators.operator.delete_table_if_exists('table_name')

    def test_delete_table_if_mismatches(self):
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
        ut.operators.operator.create_empty_table(
            table_name='table_name_2',
            schema=schema,
            time_partitioning=time_partitioning,
            range_partitioning=range_partitioning,
            require_partition_filter=require_partition_filter,
            clustering_fields=clustering_fields)
        clustering_fields = ['a']
        ut.operators.operator.create_empty_table(
            table_name='table_name_3',
            schema=schema,
            time_partitioning=time_partitioning,
            range_partitioning=range_partitioning,
            require_partition_filter=require_partition_filter,
            clustering_fields=clustering_fields)

        ut.operators.operator_quick_setup.delete_table_if_mismatches(
            'table_name_1', 'table_name_2')
        ut.operators.operator_quick_setup.delete_table_if_mismatches(
            'table_name_1', 'table_name_3')

        self.assertTrue(ut.table.table_exists('table_name_2'))
        self.assertFalse(ut.table.table_exists('table_name_3'))

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
        self.assertTrue(ut.table.table_exists('table_name_1'))
        table = ut.table.get_table('table_name_1')
        self.assertEqual(0, table.num_rows)
        self.assertEqual(schema, table.schema)
        self.assertEqual(time_partitioning, table.time_partitioning)
        self.assertEqual(range_partitioning, table.range_partitioning)
        self.assertEqual(
            require_partition_filter, table.require_partition_filter)
        self.assertEqual(clustering_fields, table.clustering_fields)

        ut.operators.operator.create_empty_table('table_name_2')
        self.assertTrue(ut.table.table_exists('table_name_2'))
        table = ut.table.get_table('table_name_2')
        self.assertEqual(0, table.num_rows)
        self.assertEqual([], table.schema)
        self.assertEqual(None, table.time_partitioning)
        self.assertEqual(None, table.range_partitioning)
        self.assertEqual(None, table.require_partition_filter)
        self.assertEqual(None, table.clustering_fields)

        ut.table.create_empty_table('table_name_3')
        ut.operators.operator_quick_setup.create_empty_table(
            'table_name_3', pre_delete_if_exists=True, time_to_live=5)

    def test_table_is_empty(self):
        ut.table.create_empty_table('table_name')
        self.assertTrue(ut.operators.operator.table_is_empty('table_name'))
        ut.load.query_to_dataset('select 3', 'table_name')
        self.assertFalse(
            ut.operators.operator_quick_setup.table_is_empty('table_name'))

    def test_get_columns(self):
        expected = []
        ut.table.create_empty_table('table_name_1')
        computed = ut.operators.operator.get_columns('table_name_1')
        self.assertEqual(expected, computed)

        expected = ['a', 'b']
        ut.load.query_to_dataset('select 3 as a, 1 as b', 'table_name_2')
        computed = ut.operators.operator.get_columns('table_name_2')
        self.assertEqual(expected, computed)

    def test_get_table_rows(self):
        expected = [
            bigquery.Row((3, 'y'), {'a': 0, 'b': 1}),
            bigquery.Row((4, 'x'), {'a': 0, 'b': 1})]
        query = """
        select 3 as a, 'y' as b union all select 4 as a, 'x' as b
        """
        ut.load.query_to_dataset(query, 'table_name')
        computed = ut.operators.operator_quick_setup.get_table_rows(
            'table_name')
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
        ut.table.create_empty_table(
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
        computed = ut.operators.operator.get_format_attributes('table_name_1')
        self.assertEqual(expected, computed)

        expected = {
            'schema': [bigquery.SchemaField('x', 'INTEGER')],
            'time_partitioning': None,
            'range_partitioning': None,
            'require_partition_filter': None,
            'clustering_fields': None}
        ut.load.query_to_dataset('select 3 as x', 'table_name_2')
        computed = ut.operators.operator.get_format_attributes('table_name_2')
        self.assertEqual(expected, computed)

    def test_set_time_to_live(self):
        from datetime import datetime, timedelta, timezone
        expected = (
                datetime.now(timezone.utc) +
                timedelta(days=3 + 1)).date()
        expected = datetime.combine(
            expected, datetime.min.time(), tzinfo=timezone.utc)
        ut.table.create_empty_table('table_name')
        ut.operators.operator_quick_setup.set_time_to_live('table_name', 3)
        computed = ut.table.get_table('table_name').expires
        self.assertEqual(expected, computed)

import unittest
from google.cloud import bigquery
from tests import utils as ut


class DatasetWithoutApiCallsTest(unittest.TestCase):

    def test_instantiate_dataset(self):
        computed = ut.operators.operator.instantiate_dataset()
        self.assertTrue(isinstance(computed, bigquery.Dataset))
        self.assertEqual(ut.constants.project_id, computed.project)
        self.assertEqual(ut.constants.dataset_name, computed.dataset_id)


class DatasetWithApiCallsTest(ut.base_class.BaseClassTest):

    def test_get_dataset(self):
        computed = ut.operators.operator.instantiate_dataset()
        self.assertTrue(isinstance(computed, bigquery.Dataset))
        self.assertEqual(ut.constants.project_id, computed.project)
        self.assertEqual(ut.constants.dataset_name, computed.dataset_id)

    def test_dataset_exists(self):
        self.assertTrue(ut.operators.operator.dataset_exists())
        ut.dataset.delete_dataset()
        self.assertFalse(ut.operators.operator.dataset_exists())

    def test_delete_dataset(self):
        ut.operators.operator.delete_dataset()
        self.assertFalse(ut.dataset.dataset_exists())

    def test_create_dataset(self):
        ut.dataset.delete_dataset()
        ut.operators.operator.create_dataset(location='EU')
        self.assertTrue(ut.dataset.dataset_exists())
        dataset = ut.dataset.get_dataset()
        self.assertEqual('EU', dataset.location)

    def test_create_dataset_if_not_exist(self):
        ut.dataset.delete_dataset()
        ut.operators.operator.create_dataset_if_not_exist(location='EU')
        self.assertTrue(ut.dataset.dataset_exists())
        dataset = ut.dataset.get_dataset()
        self.assertEqual('EU', dataset.location)
        ut.operators.operator.create_dataset_if_not_exist(location='EU')

    def test_list_tables(self):
        self.assertEqual([], ut.operators.operator.list_tables())
        for n in ['table_name_1', 'table_name_2']:
            ut.table.create_empty_table(n)
        self.assertEqual(
            ['table_name_1', 'table_name_2'],
            ut.operators.operator.list_tables())

    def test_clean_dataset(self):
        for n in ['table_name_1', 'table_name_2']:
            ut.table.create_empty_table(n)
        ut.operators.operator.clean_dataset()
        self.assertEqual([], ut.dataset.list_tables())

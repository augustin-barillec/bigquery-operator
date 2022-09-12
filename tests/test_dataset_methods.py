import unittest
from google.cloud import bigquery
from tests import utils as ut


class DatasetMethodsWithoutApiCallsTest(unittest.TestCase):

    def test_instantiate_dataset(self):
        computed = ut.operators.operator.instantiate_dataset()
        self.assertTrue(isinstance(computed, bigquery.Dataset))
        self.assertEqual(ut.constants.project_id, computed.project)
        self.assertEqual(ut.constants.dataset_name, computed.dataset_id)


class DatasetMethodsWithApiCallsTest(ut.base_class.BaseClassTest):

    def test_get_dataset(self):
        computed = ut.operators.operator.instantiate_dataset()
        self.assertTrue(isinstance(computed, bigquery.Dataset))
        self.assertEqual(ut.constants.project_id, computed.project)
        self.assertEqual(ut.constants.dataset_name, computed.dataset_id)

    def test_dataset_exists(self):
        self.assertTrue(ut.operators.operator.dataset_exists())
        ut.delete.delete_dataset()
        self.assertFalse(ut.operators.operator.dataset_exists())

    def test_delete_dataset(self):
        ut.operators.operator.delete_dataset()
        self.assertFalse(ut.exist.dataset_exists())

    def tests_create_dataset(self):
        ut.delete.delete_dataset()
        ut.operators.operator.create_dataset()
        self.assertTrue(ut.exist.dataset_exists())

        ut.delete.delete_dataset()
        ut.operators.operator.create_dataset(
            location='EU', default_time_to_live=11)
        self.assertTrue(ut.exist.dataset_exists())
        dataset = ut.get.get_dataset()
        self.assertEqual('EU', dataset.location)
        self.assertEqual(11*24*3600*1000, dataset.default_table_expiration_ms)

    def test_clean_dataset(self):
        for n in ['table_name_1', 'table_name_2']:
            ut.create.create_empty_table(n)
        ut.operators.operator.clean_dataset()
        self.assertEqual([], ut.list.list_table_names())

import unittest
from bigquery_operator import Operator
from tests import utils as ut


class GettersTest(unittest.TestCase):

    def test_call_operator_getters(self):
        o = Operator(
            ut.constants.bq_client,
            'project_id_1.dataset_name_1')
        self.assertEqual(ut.constants.bq_client, o.client)
        self.assertEqual(ut.constants.project_id, o.client_project_id)

        self.assertEqual('project_id_1.dataset_name_1', o.dataset_id)
        self.assertEqual('project_id_1', o.dataset_project_id)
        self.assertEqual('dataset_name_1', o.dataset_name)

    def test_call_operator_quick_setup_getter(self):
        o = ut.operators.operator_quick_setup
        self.assertEqual(ut.constants.project_id, o.project_id)

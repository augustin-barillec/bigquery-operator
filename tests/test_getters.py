import unittest
from tests import utils as ut


class GettersTest(unittest.TestCase):

    def test_call_operator_getters(self):
        o = ut.operators.operator
        self.assertEqual(ut.constants.project_id, o.project_id)
        self.assertEqual(ut.constants.bq_client, o.client)
        self.assertEqual(ut.constants.dataset_id, o.dataset_id)
        self.assertEqual(ut.constants.dataset_name, o.dataset_name)

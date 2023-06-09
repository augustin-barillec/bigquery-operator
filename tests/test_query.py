import unittest
from google.cloud import bigquery
from tests import utils as ut


class QueryTest(unittest.TestCase):

    def test_sample_query(self):
        expected = (
            'select * from (select 3) '
            'where rand() < 100/(select count(*) from (select 3))')
        computed = ut.operators.operator_quick_setup.sample_query(
            query='select 3', size=100)
        self.assertEqual(expected, computed)

    def test_get_query_rows(self):
        expected = [bigquery.Row((5, 'y'), {'a': 0, 'b': 1}), bigquery.Row((4, 'x'), {'a': 0, 'b': 1})]
        query = """
        select 5 as a, 'y' as b union all select 4 as a, 'x' as b
        """
        computed = ut.operators.operator_quick_setup.get_query_rows(query)
        self.assertEqual(expected, computed)
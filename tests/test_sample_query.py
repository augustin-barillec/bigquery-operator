import unittest
from tests import utils as ut


class SampleQueryTest(unittest.TestCase):

    def test_sample_query(self):
        expected = (
            'select * from (select 3) '
            'where rand() < 100/(select count(*) from (select 3))')
        computed = ut.operators.operator_quick_setup.sample_query(
            query='select 3', size=100)
        self.assertEqual(expected, computed)

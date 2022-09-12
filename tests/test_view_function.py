from tests import utils as ut


class ViewFunctionsTest(ut.base_class.BaseClassTest):

    def test_create_view(self):
        ut.operators.operator.create_view('select 3', 'table_name')
        self.assertTrue(ut.table.table_exists('table_name'))
        table = ut.table.get_table('table_name')
        self.assertEqual('select 3', table.view_query)

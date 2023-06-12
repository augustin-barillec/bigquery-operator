from tests import utils as ut


class ViewTest(ut.base_class.BaseClassTest):
    def test_create_view(self):
        ut.operators.operator.create_view(
            'select 3', 'table_name_1', time_to_live=1)
        self.assertTrue(ut.table.table_exists('table_name_1'))
        table = ut.table.get_table('table_name_1')
        self.assertEqual('select 3', table.view_query)

        ut.table.create_empty_table('table_name_2')
        ut.operators.operator.create_view(
            'select 4', 'table_name_2', pre_delete_if_exists=True)
        self.assertTrue(ut.table.table_exists('table_name_2'))

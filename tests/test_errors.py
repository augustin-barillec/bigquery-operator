import unittest
import bigquery_operator
from unittest import mock
from copy import deepcopy
from google.api_core.exceptions import PreconditionFailed
from tests import utils as ut


class ErrorsTest(unittest.TestCase):
    def test_raise_error_if_dataset_id_not_contain_exactly_one_dot(self):
        msg = 'dataset_id must contain exactly one dot'

        with self.assertRaises(ValueError) as cm:
            bigquery_operator.Operator(
                client=ut.constants.bq_client,
                dataset_id='a')
        self.assertEqual(msg, str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            bigquery_operator.Operator(
                client=ut.constants.bq_client,
                dataset_id='a.b.c')
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_queries_empty(self):
        with self.assertRaises(ValueError) as cm:
            ut.operators.operator_quick_setup.run_queries(
                queries=[],
                destination_table_names=[])
        msg = 'queries must not be empty'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_queries_dtns_different_lengths(self):
        with self.assertRaises(ValueError) as cm:
            ut.operators.operator.run_queries(
                queries=['select 3'],
                destination_table_names=['table_name_1', 'table_name_2'])
        msg = 'queries and destination_table_names must have the same length'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_source_table_names_empty_for_extract(self):
        with self.assertRaises(ValueError) as cm:
            ut.operators.operator.extract_tables(
                source_table_names=[],
                destination_uris=[])
        msg = 'source_table_names must not be empty'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_stns_dus_different_lengths(self):
        with self.assertRaises(ValueError) as cm:
            ut.operators.operator.extract_tables(
                source_table_names=['table_name_1', 'table_name_2'],
                destination_uris=['uri_1'])
        msg = ('source_table_names and destination_uris '
               'must have the same length')
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_source_uris_empty(self):
        with self.assertRaises(ValueError) as cm:
            ut.operators.operator_quick_setup.load_tables(
                source_uris=[],
                destination_table_names=[])
        msg = 'source_uris must not be empty'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_suis_dtns_different_lengths(self):
        with self.assertRaises(ValueError) as cm:
            ut.operators.operator.load_tables(
                source_uris=['uri_1', 'uri_2', 'uri_3'],
                destination_table_names=['table_name_1', 'table_name_2'])
        msg = ('source_uris and destination_table_names '
               'must have the same length')
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_source_table_names_empty_for_copy(self):
        with self.assertRaises(ValueError) as cm:
            ut.operators.operator.copy_tables(
                source_table_names=[],
                destination_table_names=[])
        msg = 'source_table_names must not be empty'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_stns_dtns_different_lengths(self):
        with self.assertRaises(ValueError) as cm:
            ut.operators.operator.copy_tables(
                source_table_names=['table_name_1'],
                destination_table_names=[])
        msg = ('source_table_names and destination_table_names '
               'must have the same length')
        self.assertEqual(msg, str(cm.exception))

    @mock.patch('tests.utils.operators.operator.get_table')
    @mock.patch('tests.utils.operators.operator._client.update_table')
    def test_set_time_to_live_raises_exception_after_max_retries(
            self, mock_update_table, mock_get_table):
        from google.api_core.exceptions import PreconditionFailed
        from copy import deepcopy

        mock_table = mock.MagicMock()
        mock_table.expires = None
        mock_table_1 = deepcopy(mock_table)
        mock_table_2 = deepcopy(mock_table)
        mock_table_3 = deepcopy(mock_table)
        mock_get_table.side_effect = [mock_table_1, mock_table_2, mock_table_3]
        mock_update_table.side_effect = PreconditionFailed(
            "PreconditionFailed")
        with self.assertRaises(PreconditionFailed):
            ut.operators.operator.set_time_to_live("table_name", 7, [1, 1])

        self.assertEqual(mock_update_table.call_count, 3)

    @mock.patch('tests.utils.operators.operator.get_table')
    @mock.patch('tests.utils.operators.operator._client.update_table')
    def test_set_time_to_live_catches_2_exceptions(
            self, mock_update_table, mock_get_table):
        from datetime import datetime, timedelta, timezone
        expected = (
                datetime.now(timezone.utc) +
                timedelta(days=7 + 1)).date()
        expected = datetime.combine(
            expected, datetime.min.time(), tzinfo=timezone.utc)

        mock_table = mock.MagicMock()
        mock_table.expires = None
        mock_table_1 = deepcopy(mock_table)
        mock_table_2 = deepcopy(mock_table)
        mock_table_3 = deepcopy(mock_table)
        mock_table_3.expires = expected
        mock_get_table.side_effect = [mock_table_1, mock_table_2, mock_table_3]
        mock_update_table.side_effect = PreconditionFailed(
            "PreconditionFailed")
        ut.operators.operator.set_time_to_live("table_name", 7, [1, 1])
        self.assertEqual(mock_update_table.call_count, 2)

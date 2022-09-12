from tests.utils import constants
from bigquery_operator import Operator, OperatorQuickSetup

operator = Operator(
    client=constants.bq_client,
    dataset_id=constants.dataset_id)

operator_quick_setup = OperatorQuickSetup(
    project_id=constants.project_id,
    dataset_name=constants.dataset_name)

from typing import Optional
from google.auth import credentials as cred
from google.cloud import bigquery
from bigquery_operator.operator import Operator


class OperatorQuickSetup(Operator):
    """The purpose of this class is to quickly set up an Operator.

    An instance of OperatorQuickSetup is simply an instance of the base class
    built with the following arguments:

    ::

         client=client
         dataset_id=dataset_id

    where

    ::

        client = google.cloud.bigquery.Client(
            project=project_id,
            credentials=credentials)
        dataset_id = project_id + '.' + dataset_name

    Args:
        project_id (str, optional): The project id.
        dataset_name (str, optional): The dataset name.
        credentials (google.auth.credentials.Credentials): Credentials used to
            build the client. If not passed, falls back to the default inferred
            from the environment.
    """
    def __init__(
            self,
            project_id: str,
            dataset_name: str,
            credentials: Optional[cred.Credentials] = None) -> None:
        self._project_id = project_id
        client = bigquery.Client(
            project=self._project_id,
            credentials=credentials)
        dataset_id = f'{self._project_id}.{dataset_name}'
        super().__init__(client, dataset_id)

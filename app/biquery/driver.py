from typing import Any

from google.cloud.bigquery import Client, QueryJob, Table
from google.oauth2 import service_account

KEY_PATH = "service_account.json"


class Driver:

    def __init__(self, project_name: str):
        self.project_name = project_name
        self.client = self.get_client()

    def get_client(self):
        credentials = service_account.Credentials.from_service_account_file(
            KEY_PATH,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return Client(credentials=credentials, project=self.project_name)

    def get_table_by_name(self, dataset: str, name: str) -> Table:
        table_name = f"{self.project_name}.{dataset}.{name}"
        return self.client.get_table(table_name)


    @staticmethod
    def escaped_table_id(table: Table) -> str:
        return f"`{table.project}.{table.dataset_id}.{table.table_id}`"


    def run_query(self, query: str) -> Any:
        return self.get_scalar_result(self.client.query(query))

    @staticmethod
    def get_scalar_result(query_job: QueryJob) -> Any:
        res = query_job.result()
        assert res.total_rows == 1
        return [r for r in res][0].values()[0]

from typing import Any, Dict

from google.cloud.bigquery import Client, QueryJob, Table
from google.oauth2 import service_account
from biquery.metric_model import MetricModel

KEY_PATH = "service_account.json"


class Driver:

    def __init__(self, project_name: str):
        self.project_name = project_name
        self.client = self.get_client()
        self.query_cache: Dict[str, str] = {}  # mainly used as introspection cache for testing right now

    def get_client(self):
        credentials = service_account.Credentials.from_service_account_file(
            KEY_PATH,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return Client(credentials=credentials, project=self.project_name)

    def get_table_by_name(self, dataset: str, name: str) -> Table:
        table_name = f"{self.project_name}.{dataset}.{name}"
        return self.client.get_table(table_name)

    def run_query(self, query: str) -> Any:
        return self.get_scalar_result(self.client.query(query))

    @staticmethod
    def get_scalar_result(query_job: QueryJob) -> Any:
        res = query_job.result()
        assert res.total_rows == 1
        return [r for r in res][0].values()[0]

    def build_query(self, model: MetricModel) -> str:
        field = model.field
        agg = model.agg.value.format(column=field.column_name)
        table = f"`{self.project_name}.{field.dataset}.{field.table}`"
        query = f"SELECT {agg} FROM {table}"
        self.query_cache[model.name] = query
        return query

from typing import Any, Dict

from biquery.builder import QueryBuilder
from biquery.driver import Driver
from biquery.metric_model import Field, MetricModel, Aggregation


class Engine:
    def __init__(self, project: str):
        self.models = {}
        self.driver = Driver(project)
        self.builder = QueryBuilder()

    def load_model_conf(self, model_conf: Dict[str, Any]):
        field_conf = model_conf["field"]
        table = self.driver.get_table_by_name(field_conf["dataset"], field_conf["table"])
        field = Field(table, field_conf["column"])
        agg = Aggregation[model_conf["aggregation"].upper()]
        model = MetricModel(model_conf["name"], field, agg)
        self.models[model_conf["name"]] = model

    def run_model(self, model_name: str) -> Any:
        query = self.builder.build_query(self.models[model_name])
        return self.driver.run_query(query)

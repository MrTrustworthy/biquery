from typing import Dict

from biquery.metric_model import MetricModel
from biquery.driver import Driver


class QueryBuilder:

    def __init__(self):
        self.query_cache: Dict[str, str] = {}  # mainly used as introspection cache for testing right now

    def build_query(self, model: MetricModel) -> str:
        agg = model.agg.value.format(column=model.field.column_name)
        table = Driver.escaped_table_id(model.field.table)
        query = f"SELECT {agg} FROM {table}"
        self.query_cache[model.name] = query
        return query
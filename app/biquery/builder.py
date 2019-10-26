from biquery.metric_model import MetricModel
from biquery.driver import Driver


class QueryBuilder:

    def build_query(self, model: MetricModel) -> str:
        agg = model.agg.value.format(column=model.field.column_name)
        table = Driver.escaped_table_id(model.field.table)
        return f"SELECT {agg} FROM {table}"

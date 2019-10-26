from biquery.builder import QueryBuilder
from biquery.driver import Driver
from biquery.metric_model import MetricModel, Field, Aggregation
from tests.conftest import PROJECT


def test_simple_model():
    driver = Driver(PROJECT)
    table = driver.get_table_by_name("test", "orders")
    field = Field(table, "id")
    model = MetricModel("test_simple_sum", field, Aggregation.COUNT)
    builder = QueryBuilder()
    query = builder.build_query(model)
    assert query == f"SELECT count(id) FROM {Driver.escaped_table_id(table)}"
    result = driver.run_query(query)
    assert result == 1000

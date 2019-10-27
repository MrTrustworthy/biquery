from biquery.builder import QueryBuilder
from biquery.driver import Driver
from biquery.metric_model import MetricModel, Field, Aggregation
from tests.conftest import PROJECT


def test_simple_count_model():
    driver = Driver(PROJECT)
    table = driver.get_table_by_name("test", "orders")
    table_esc = Driver.escaped_table_id(table)
    field = Field(table, "id")
    model = MetricModel("test_simple_count", field, Aggregation.COUNT)
    builder = QueryBuilder()
    query = builder.build_query(model)
    assert query == f"SELECT COUNT(id) FROM {table_esc}"
    result = driver.run_query(query)
    assert result == 1000


def test_join_sum_model():
    driver = Driver(PROJECT)
    order_table = driver.get_table_by_name("test", "orders")
    order_table_esc = Driver.escaped_table_id(order_table)
    product_table = driver.get_table_by_name("test", "products")
    product_table_esc = Driver.escaped_table_id(product_table)

    field = Field(order_table, "price")
    model = MetricModel("test_simple_sum", field, Aggregation.SUM)
    builder = QueryBuilder()
    query = builder.build_query(model)

    assert query == f"SELECT SUM(price) FROM {order_table_esc} LEFT JOIN {product_table_esc} ON {product_table_esc}.id = {order_table_esc}.product_id"
    result = driver.run_query(query)
    assert result == 1000

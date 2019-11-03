import pytest
from biquery.engine import Engine
from tests.conftest import PROJECT


def test_simple_count_model():
    model_name = "test_simple_count_model"
    model_conf = {
        "name": model_name,
        "field": {
            "dataset": "test",
            "table": "orders",
            "column": "id"
        },
        "aggregation": "count"
    }
    engine = Engine(PROJECT)
    engine.load_model_conf(model_conf)
    result = engine.run_model(model_name)
    query = engine.driver.query_cache[model_name]
    assert query == f"SELECT COUNT(id) FROM `{PROJECT}.test.orders`"
    assert result == 1000


@pytest.mark.skip
def test_1to1_reversed_join_sum_model():
    model_name = "test_1to1_reversed_join_sum_model"
    model_conf = {
        "name": model_name,
        "field": {
            "dataset": "test",
            "table": "products",
            "column": "price"
        },
        "layout": {


        },
        "aggregation": "sum"
    }
    engine = Engine(PROJECT)
    engine.load_model_conf(model_conf)
    result = engine.run_model(model_name)
    query = engine.driver.query_cache[model_name]

    assert query == f"select sum(`{PROJECT}.test.orders`.price) from `{PROJECT}.test.orders` left join `{PROJECT}.test.products` on `{PROJECT}.test.products`.id = `{PROJECT}.test.orders`.product_id"
    assert int(result) == 241531


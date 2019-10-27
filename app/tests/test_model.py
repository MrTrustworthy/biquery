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
    query = engine.builder.query_cache[model_name]
    assert query == f"SELECT COUNT(id) FROM `{PROJECT}.test.orders`"
    assert result == 1000


def test_1to1_reversed_join_sum_model():
    model_name = "test_1to1_reversed_join_sum_model"
    model_conf = {
        "name": model_name,
        "field": {
            "dataset": "test",
            "table": "products",
            "column": "price"
        },
        "aggregation": "sum"
    }
    engine = Engine(PROJECT)
    engine.load_model_conf(model_conf)
    result = engine.run_model(model_name)
    query = engine.builder.query_cache[model_name]

    assert query == "select sum(price) from `nullpointer-184019.test.orders` left join `nullpointer-184019.test.products` on `nullpointer-184019.test.products`.id = `nullpointer-184019.test.orders`.product_id"
    assert int(result) == 241531


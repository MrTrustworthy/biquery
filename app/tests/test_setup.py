from typing import List

from biquery import __version__
from biquery.driver import Driver
from google.cloud.bigquery import Client
from google.cloud.bigquery import Table


def test_version():
    assert __version__ == '0.1.0'


def test_tables_correct(client: Client, bq_tables: List[Table]):
    counts = set()
    for table in bq_tables:
        job = client.query(f"SELECT COUNT(*) FROM {Driver.escaped_table_id(table)}")
        counts.add(Driver.get_scalar_result(job))
    assert counts == {1000, 100, 6}



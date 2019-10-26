import pathlib

import pytest
from biquery.driver import Driver
from google.cloud import bigquery as bq
from google.cloud.bigquery import LoadJobConfig, SourceFormat
from google.cloud.bigquery.dataset import Dataset

KEY_PATH = "service_account.json"
PROJECT = "nullpointer-184019"
TEST_DS = "test"
TEST_DATA_DIR = pathlib.Path("tests/data")


@pytest.fixture()
def client():
    driver = Driver(PROJECT)
    yield driver.client


@pytest.fixture()
def bq_dataset(client: bq.Client):
    client.delete_dataset(TEST_DS, delete_contents=True, not_found_ok=True)
    client.create_dataset(TEST_DS, exists_ok=False)
    ds: Dataset = client.get_dataset(TEST_DS)
    assert ds is not None
    yield ds


@pytest.fixture()
def bq_tables(client: bq.Client, bq_dataset: Dataset):
    table_names = [d.resolve().stem for d in TEST_DATA_DIR.glob("*.csv")]
    tables = [client.create_table(fqid(table_name)) for table_name in table_names]

    job_config = LoadJobConfig(source_format=SourceFormat.CSV, skip_leading_rows=1, autodetect=True)
    jobs = []
    for table_name, table in zip(table_names, tables):
        with open(TEST_DATA_DIR / f"{table_name}.csv", "rb") as table_data:
            job = client.load_table_from_file(table_data, table, job_config=job_config)
        jobs.append(job)
    [job.result() for job in jobs]  # wait for the jobs to complete
    yield tables


def fqid(table_name: str) -> str:
    return f"{PROJECT}.{TEST_DS}.{table_name}"

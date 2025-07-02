from pandas import DataFrame
from scoda.api.db import DB
from collections import defaultdict
from time import time
import scoda.api.dataset as scoda_dataset


def benchmark_write_all_tables(
    db: DB,
    datasets: list[scoda_dataset.Dataset],
) -> float:
    def _run() -> None:
        dataset: scoda_dataset.Dataset
        for dataset in datasets:
            dataset.data.to_sql(
                name=dataset.name,
                con=db.engine,
                if_exists="append",
                index=False,
            )

    start_time: float = time()
    _run()
    end_time: float = time()

    db.recreate_tables()

    return end_time - start_time


def benchmark_per_db_table_write(
    db: DB,
    dataset: scoda_dataset.Dataset,
) -> float:
    def _run() -> None:
        dataset.data.to_sql(
            name=dataset.name,
            con=db.engine,
            if_exists="append",
            index=False,
        )

    start_time: float = time()
    _run()
    end_time: float = time()

    db.recreate_tables()

    return end_time - start_time


def benchmark_max_query(db: DB) -> DataFrame:
    pass


def benchmark_min_query(db: DB) -> DataFrame:
    pass


def benchmark_average_query(db: DB) -> DataFrame:
    pass


def benchmark_query(db: str, sql: str) -> DataFrame:
    pass

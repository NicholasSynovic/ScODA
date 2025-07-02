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
    db_name: str,
    dfs: dict[str, DataFrame],
    iterations: int,
) -> DataFrame:
    def _run(name: str, df: DataFrame) -> float:
        start_time: float = time()
        df.to_sql(name=name, con=db.engine, if_exists="append", index=False)
        end_time: float = time()
        return end_time - start_time

    data: dict[str, list[float]] = defaultdict(list)
    for _ in range(iterations):
        name: str
        df: DataFrame
        for name, df in dfs.items():
            key: str = f"{name}_{db_name}"
            data[key].append(_run(name=name, df=df))

    return DataFrame(data=data)


def benchmark_max_query(db: DB) -> DataFrame:
    pass


def benchmark_min_query(db: DB) -> DataFrame:
    pass


def benchmark_average_query(db: DB) -> DataFrame:
    pass


def benchmark_query(db: str, sql: str) -> DataFrame:
    pass

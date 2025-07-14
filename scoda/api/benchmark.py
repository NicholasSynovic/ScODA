from pandas import DataFrame
from scoda.api.db import DB
from time import time
import scoda.api.dataset as scoda_dataset
from collections.abc import Iterator
from progress.bar import Bar
from collections import defaultdict


def benchmark_write_all_tables(
    db: DB,
    iterations: int,
    datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
) -> None:
    data = defaultdict(list)

    def _run() -> None:
        dataset: scoda_dataset.Dataset
        for dataset in datasets:
            dataset.data.to_sql(
                name=dataset.name,
                con=db.engine,
                if_exists="append",
                index=False,
            )

    with Bar("Benchmarking writing all tables to the database", max=iterations) as bar:
        for _ in range(iterations):
            start_time: float = time()
            _run()
            end_time: float = time()
            db.recreate_tables()
            data["seconds"].append(end_time - start_time)
            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_write_all_tables",
        con=db.engine,
        if_exists="append",
        index=False,
    )


def benchmark_write_per_table(
    db: DB,
    iterations: int,
    datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
) -> None:
    data: dict[str, list[float]] = defaultdict(list)

    def _run() -> None:
        dataset.data.to_sql(
            name=dataset.name,
            con=db.engine,
            if_exists="append",
            index=False,
        )

    with Bar(
        "Benchmarking writing individual tables to database",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            dataset: scoda_dataset.Dataset
            for dataset in datasets:
                start_time: float = time()
                _run()
                end_time: float = time()
                db.recreate_tables()
                data[dataset.name].append(end_time - start_time)
                bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_per_table_write",
        con=db.engine,
        if_exists="append",
        index=False,
    )


def benchmark_sequential_writes_per_table(
    db: DB, dataset: scoda_dataset.Dataset
) -> DataFrame:
    pass


def benchmark_max_query(db: DB) -> DataFrame:
    pass


def benchmark_min_query(db: DB) -> DataFrame:
    pass


def benchmark_average_query(db: DB) -> DataFrame:
    pass


def benchmark_query(db: str, sql: str) -> DataFrame:
    pass

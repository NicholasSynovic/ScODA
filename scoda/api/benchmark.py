from pandas import DataFrame
from scoda.api.db import DB
from time import time
import scoda.api.dataset as scoda_dataset
from collections.abc import Iterator
from progress.bar import Bar
from collections import defaultdict


def benchmark_write_all_tables(
    test_db: DB,
    iterations: int,
    benchmark_db: DB,
    datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
) -> None:
    data = defaultdict(list)

    def _run(
        datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
    ) -> None:
        ds: scoda_dataset.Dataset
        for ds in datasets:
            ds.data.to_sql(
                name=ds.name,
                con=test_db.engine,
                if_exists="append",
                index=False,
            )

    with Bar("Benchmarking writing all tables to the database", max=iterations) as bar:
        for _ in range(iterations):
            start_time: float = time()
            _run(datasets=datasets)
            end_time: float = time()
            test_db.recreate_tables()
            data["seconds"].append(end_time - start_time)
            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_write_all_tables",
        con=benchmark_db.engine,
        if_exists="append",
        index=False,
    )


def benchmark_write_per_table(
    test_db: DB,
    iterations: int,
    benchmark_db: DB,
    datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
) -> None:
    data: dict[str, list[float]] = defaultdict(list)

    def _run(ds: scoda_dataset.Dataset) -> None:
        ds.data.to_sql(
            name=dataset.name,
            con=test_db.engine,
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
                _run(ds=dataset)
                end_time: float = time()
                data[dataset.name].append(end_time - start_time)
                bar.next()
            test_db.recreate_tables()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_per_table_write",
        con=benchmark_db.engine,
        if_exists="append",
        index=False,
    )


def benchmark_sequential_writes_all_tables(
    test_db: DB,
    iterations: int,
    benchmark_db: DB,
    datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
) -> None:
    data: dict[str, list[float]] = defaultdict(list)

    def _run(
        datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
    ) -> None:
        ds: scoda_dataset.Dataset
        for ds in datasets:
            ds.data.to_sql(
                name=ds.name,
                con=test_db.engine,
                if_exists="append",
                index=False,
                chunksize=1,
            )

    with Bar(
        "Benchmarking writing all tables to the database one row at a time",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            start_time: float = time()
            _run(datasets=datasets)
            end_time: float = time()
            test_db.recreate_tables()
            data["seconds"].append(end_time - start_time)
            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_write_all_tables_row_by_row",
        con=benchmark_db.engine,
        if_exists="append",
        index=False,
    )


def benchmark_sequential_writes_per_tables(
    test_db: DB,
    iterations: int,
    benchmark_db: DB,
    datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
) -> None:
    data: dict[str, list[float]] = defaultdict(list)

    def _run(ds: scoda_dataset.Dataset) -> None:
        ds.data.to_sql(
            name=ds.name,
            con=test_db.engine,
            if_exists="append",
            index=False,
            chunksize=1,
        )

    with Bar(
        "Benchmarking writing sequential row writing per table",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            dataset: scoda_dataset.Dataset
            for dataset in datasets:
                start_time: float = time()
                _run(ds=dataset)
                end_time: float = time()
                data[dataset.name].append(end_time - start_time)
                bar.next()
            test_db.recreate_tables()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_write_per_table_row_by_row",
        con=benchmark_db.engine,
        if_exists="append",
        index=False,
    )


def benchmark_min_query(db: DB) -> DataFrame:
    pass


def benchmark_average_query(db: DB) -> DataFrame:
    pass


def benchmark_query(db: str, sql: str) -> DataFrame:
    pass

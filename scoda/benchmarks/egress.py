"""
Data egress benchmarks.

Copyright 2025 (C) Nicholas M. Synovic

"""

from collections import defaultdict
from time import time

from pandas import DataFrame
from progress.bar import Bar

import scoda.datasets as scoda_dataset
import scoda.db as scoda_db
import scoda.db.results as scoda_results


def batch_read_all_tables(
    test_db: scoda_db.DB,
    iterations: int,
    results_db: scoda_results.Results,
    datasets: Iterable[scoda_dataset.Dataset],
) -> None:
    data = defaultdict(list)

    def _run() -> None:
        ds: scoda_dataset.Dataset
        for ds in datasets:
            test_db.batch_read(table_name=ds.name)

    with Bar(
        "Benchmarking reading all tables from the database...",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            start_time: float = time()
            _run()
            end_time: float = time()
            data["seconds"].append(end_time - start_time)
            test_db.recreate()
            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_total_time_to_batch_read_tables",
        con=results_db.engine,
        if_exists="append",
        index=False,
    )


def batch_read_individual_tables(
    test_db: scoda_db.DB,
    iterations: int,
    results_db: scoda_results.Results,
    datasets: Iterable[scoda_dataset.Dataset],
) -> None:
    data: dict[str, list[float]] = defaultdict(list)

    def _run(ds: scoda_dataset.Dataset) -> None:
        test_db.batch_read(table_name=ds.name)

    with Bar(
        "Benchmarking reading individual tables from the database...",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            dataset: scoda_dataset.Dataset
            for dataset in datasets:
                start_time: float = time()
                _run(ds=dataset)
                end_time: float = time()
                data[dataset.name].append(end_time - start_time)
            test_db.recreate()
            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_total_time_to_batch_read_individual_tables",
        con=results_db.engine,
        if_exists="append",
        index=False,
    )


def sequential_read_all_tables(
    test_db: scoda_db.DB,
    iterations: int,
    results_db: scoda_results.Results,
    datasets: Iterable[scoda_dataset.Dataset],
) -> None:
    data: dict[str, list[float]] = defaultdict(list)

    def _run() -> None:
        ds: scoda_dataset.Dataset
        for ds in datasets:
            test_db.sequential_read(table_name=ds.name, rows=1)

    with Bar(
        "Benchmarking reading all tables from the database one row at a time...",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            start_time: float = time()
            _run()
            end_time: float = time()
            test_db.recreate()
            data["seconds"].append(end_time - start_time)
            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_total_time_to_sequential_read_tables",
        con=results_db.engine,
        if_exists="append",
        index=False,
    )


def sequential_read_individual_tables(
    test_db: scoda_db.DB,
    iterations: int,
    results_db: scoda_results.Results,
    datasets: Iterable[scoda_dataset.Dataset],
) -> None:
    data: dict[str, list[float]] = defaultdict(list)

    def _run(ds: scoda_dataset.Dataset) -> None:
        test_db.sequential_read(table_name=ds.name, rows=1)

    with Bar(
        "Benchmarking reading individual tables from the database one row at a time...",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            dataset: scoda_dataset.Dataset
            for dataset in datasets:
                start_time: float = time()
                _run(ds=dataset)
                end_time: float = time()
                data[dataset.name].append(end_time - start_time)
            test_db.recreate()
            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_total_time_to_sequential_read_individual_tables",
        con=results_db.engine,
        if_exists="append",
        index=False,
    )

"""
Data aggregation benchmarks.

Copyright 2025 (C) Nicholas M. Synovic

"""

from collections import defaultdict
from time import time

from pandas import DataFrame
from progress.bar import Bar

import scoda.datasets as scoda_dataset
import scoda.db as scoda_db
import scoda.db.results as scoda_results


def query_min_value(
    test_db: scoda_db.DB,
    iterations: int,
    results_db: scoda_results.Results,
    datasets: list[scoda_dataset.Dataset],
) -> None:
    # FIXME: Make this benchmark functional
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


def query_max_value(
    test_db: scoda_db.DB,
    iterations: int,
    results_db: scoda_results.Results,
    datasets: list[scoda_dataset.Dataset],
) -> None:
    # FIXME: Make this benchmark functional
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


def query_average_value(
    test_db: scoda_db.DB,
    iterations: int,
    results_db: scoda_results.Results,
    datasets: list[scoda_dataset.Dataset],
) -> None:
    # FIXME: Make this benchmark functional
    data = defaultdict(list)

    def _run() -> None:
        ds: scoda_dataset.Dataset
        for ds in datasets:
            test_db.query_average_value(
                table_name=ds.name,
            )

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


def query_mode_value(
    test_db: scoda_db.DB,
    iterations: int,
    results_db: scoda_results.Results,
    datasets: list[scoda_dataset.Dataset],
) -> None:
    # FIXME: Make this benchmark functional
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


def query_groupby_time_window(
    test_db: scoda_db.DB,
    iterations: int,
    results_db: scoda_results.Results,
    datasets: list[scoda_dataset.Dataset],
) -> None:
    # FIXME: Make this benchmark functional
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

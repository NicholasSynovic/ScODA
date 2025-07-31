"""
Data ingress benchmarks.

Copyright 2025 (C) Nicholas M. Synovic

"""

from collections import defaultdict
from collections.abc import Iterable
from time import time

from pandas import DataFrame
from progress.bar import Bar

import scoda.datasets.generic
import scoda.db


def batch_write_all_tables(
    test_db: scoda.db.DB,
    iterations: int,
    results_db: scoda.db.Results,
    datasets: Iterable[scoda.datasets.generic.Dataset],
) -> None:
    """
    Benchmark the batch writing of all datasets to the database.

    This function measures the total time taken to batch upload all provided
    datasets to the test database over a specified number of iterations.
    The results are then saved to a SQL table in the results database.

    Arguments:
        test_db: The database instance to benchmark against.
        iterations: The number of times to run the benchmark.
        results_db: The database instance to store the benchmark results.
        datasets: An iterable of dataset objects to upload.

    """
    data = defaultdict(list)

    def _run() -> None:
        ds: scoda.datasets.generic.Dataset
        for ds in datasets:
            test_db.batch_upload(dataset=ds)

    with Bar(
        "Benchmarking writing all tables to the database...",
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
        name="benchmark_total_time_to_batch_write_tables",
        con=results_db.engine,
        if_exists="append",
        index=False,
    )


def batch_write_individual_tables(
    test_db: scoda.db.DB,
    iterations: int,
    results_db: scoda.db.Results,
    datasets: Iterable[scoda.datasets.generic.Dataset],
) -> None:
    """
    Benchmark the batch writing of individual datasets to the database.

    This function measures the time taken to batch upload each individual
    dataset to the test database over a specified number of iterations.
    The results for each dataset are then saved to a SQL table in the results
    database.

    Arguments:
        test_db: The database instance to benchmark against.
        iterations: The number of times to run the benchmark.
        results_db: The database instance to store the benchmark results.
        datasets: An iterable of dataset objects to upload.

    """
    data: dict[str, list[float]] = defaultdict(list)

    def _run(ds: scoda.datasets.generic.Dataset) -> None:
        test_db.batch_upload(dataset=ds)

    with Bar(
        "Benchmarking writing individual tables to database...",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            dataset: scoda.datasets.generic.Dataset
            for dataset in datasets:
                start_time: float = time()
                _run(ds=dataset)
                end_time: float = time()
                data[dataset.name].append(end_time - start_time)
            test_db.recreate()
            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_total_time_to_batch_write_individual_tables",
        con=results_db.engine,
        if_exists="append",
        index=False,
    )


def sequential_write_all_tables(
    test_db: scoda.db.DB,
    iterations: int,
    results_db: scoda.db.Results,
    datasets: Iterable[scoda.datasets.generic.Dataset],
) -> None:
    """
    Benchmark the sequential writing of all datasets to the database.

    This function measures the total time taken to sequentially upload all
    provided datasets to the test database (row by row) over a specified number
    of iterations. The results are then saved to a SQL table in the results
    database.

    Arguments:
        test_db: The database instance to benchmark against.
        iterations: The number of times to run the benchmark.
        results_db: The database instance to store the benchmark results.
        datasets: An iterable of dataset objects to upload.

    """
    data: dict[str, list[float]] = defaultdict(list)

    def _run() -> None:
        ds: scoda.datasets.generic.Dataset
        for ds in datasets:
            test_db.sequential_upload(dataset=ds)

    with Bar(
        "Benchmarking writing all tables to the database one row at a time...",
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
        name="benchmark_total_time_to_sequential_write_tables",
        con=results_db.engine,
        if_exists="append",
        index=False,
    )


def sequential_write_individual_tables(
    test_db: scoda.db.DB,
    iterations: int,
    results_db: scoda.db.Results,
    datasets: Iterable[scoda.datasets.generic.Dataset],
) -> None:
    """
    Benchmark the sequential writing of individual datasets to the database.

    This function measures the time taken to sequentially upload each individual
    dataset to the test database (row by row) over a specified number of
    iterations. The results for each dataset are then saved to a SQL table in
    the results database.

    Arguments:
        test_db: The database instance to benchmark against.
        iterations: The number of times to run the benchmark.
        results_db: The database instance to store the benchmark results.
        datasets: An iterable of dataset objects to upload.

    """
    data: dict[str, list[float]] = defaultdict(list)

    def _run(ds: scoda.datasets.generic.Dataset) -> None:
        test_db.sequential_upload(dataset=ds)

    with Bar(
        "Benchmarking writing sequential row writing per table",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            dataset: scoda.datasets.generic.Dataset
            for dataset in datasets:
                start_time: float = time()
                _run(ds=dataset)
                end_time: float = time()
                data[dataset.name].append(end_time - start_time)
            test_db.recreate()
            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_total_time_to_sequential_write_individual_tables",
        con=results_db.engine,
        if_exists="append",
        index=False,
    )

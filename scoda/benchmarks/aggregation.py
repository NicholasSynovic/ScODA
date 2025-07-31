"""
Data aggregation benchmarks.

Copyright 2025 (C) Nicholas M. Synovic

"""

from collections import defaultdict
from collections.abc import Iterable
from time import time

from pandas import DataFrame
from progress.bar import Bar

import scoda.datasets.generic
import scoda.db


def query_min_value(
    test_db: scoda.db.DB,
    iterations: int,
    results_db: scoda.db.Results,
    datasets: Iterable[scoda.datasets.generic.Dataset],
) -> None:
    """
    Benchmark querying the minimum value for a specific column per table.

    This function measures the time taken to query the minimum value of a
    specified column for each dataset's corresponding table in the test database.
    The benchmark runs for a given number of iterations, and the results are
    saved to a SQL table in the results database.

    Arguments:
        test_db: The database instance to benchmark against.
        iterations: The number of times to run the benchmark.
        results_db: The database instance to store the benchmark results.
        datasets: An iterable of dataset objects, where each dataset's name
            corresponds to a table and its `query_column` specifies the column
            to query.

    """
    data = defaultdict(list)

    def _run() -> None:
        ds: scoda.datasets.generic.Dataset
        for ds in datasets:
            test_db.query_min_value(table_name=ds.name, column_name=ds.query_column)

    with Bar(
        "Benchmarking querying min value per table...",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            start_time: float = time()
            _run()
            end_time: float = time()
            data["seconds"].append(end_time - start_time)

            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_query_min_value_per_table",
        con=results_db.engine,
        if_exists="append",
        index=False,
    )


def query_max_value(
    test_db: scoda.db.DB,
    iterations: int,
    results_db: scoda.db.Results,
    datasets: Iterable[scoda.datasets.generic.Dataset],
) -> None:
    """
    Benchmark querying the maximum value for a specific column per table.

    This function measures the time taken to query the maximum value of a
    specified column for each dataset's corresponding table in the test
    database. The benchmark runs for a given number of iterations, and the
    results are saved to a SQL table in the results database.

    Arguments:
        test_db: The database instance to benchmark against.
        iterations: The number of times to run the benchmark.
        results_db: The database instance to store the benchmark results.
        datasets: An iterable of dataset objects, where each dataset's name
            corresponds to a table and its `query_column` specifies the column
            to query.

    """
    data = defaultdict(list)

    def _run() -> None:
        ds: scoda.datasets.generic.Dataset
        for ds in datasets:
            test_db.query_max_value(
                table_name=ds.name,
                column_name=ds.query_column,
            )

    with Bar(
        "Benchmarking querying max value per table...",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            start_time: float = time()
            _run()
            end_time: float = time()
            data["seconds"].append(end_time - start_time)

            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_query_max_value_per_table",
        con=results_db.engine,
        if_exists="append",
        index=False,
    )


def query_average_value(
    test_db: scoda.db.DB,
    iterations: int,
    results_db: scoda.db.Results,
    datasets: Iterable[scoda.datasets.generic.Dataset],
) -> None:
    """
    Benchmark querying the average value for a specific column per table.

    This function measures the time taken to query the average value of a
    specified column for each dataset's corresponding table in the test database.
    The benchmark runs for a given number of iterations, and the results are
    saved to a SQL table in the results database.

    Arguments:
        test_db: The database instance to benchmark against.
        iterations: The number of times to run the benchmark.
        results_db: The database instance to store the benchmark results.
        datasets: An iterable of dataset objects, where each dataset's name
            corresponds to a table and its `query_column` specifies the
            column to query.

    """
    data = defaultdict(list)

    def _run() -> None:
        ds: scoda.datasets.generic.Dataset
        for ds in datasets:
            test_db.query_average_value(
                table_name=ds.name,
                column_name=ds.query_column,
            )

    with Bar(
        "Benchmarking querying average value per table...",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            start_time: float = time()
            _run()
            end_time: float = time()
            data["seconds"].append(end_time - start_time)

            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_query_average_value_per_table",
        con=results_db.engine,
        if_exists="append",
        index=False,
    )


def query_mode_value(
    test_db: scoda.db.DB,
    iterations: int,
    results_db: scoda.db.Results,
    datasets: Iterable[scoda.datasets.generic.Dataset],
) -> None:
    """
    Benchmark querying the mode value for a specific column per table.

    This function measures the time taken to query the mode value of a
    specified column for each dataset's corresponding table in the test
    database. The benchmark runs for a given number of iterations, and the
    results are saved to a SQL table in the results database.

    Arguments:
        test_db: The database instance to benchmark against.
        iterations: The number of times to run the benchmark.
        results_db: The database instance to store the benchmark results.
        datasets: An iterable of dataset objects, where each dataset's name
            corresponds to a table and its `query_column` specifies the column
            to query.

    """
    data = defaultdict(list)

    def _run() -> None:
        ds: scoda.datasets.generic.Dataset
        for ds in datasets:
            test_db.query_mode_value(
                table_name=ds.name,
                column_name=ds.query_column,
            )

    with Bar(
        "Benchmarking querying mode value per table...",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            start_time: float = time()
            _run()
            end_time: float = time()
            data["seconds"].append(end_time - start_time)

            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_query_mode_value_per_table",
        con=results_db.engine,
        if_exists="append",
        index=False,
    )


def query_groupby_time_window(
    test_db: scoda.db.DB,
    iterations: int,
    results_db: scoda.db.Results,
    datasets: Iterable[scoda.datasets.generic.Dataset],
) -> None:
    """
    Benchmark querying data grouped by time windows per table.

    This function measures the time taken to query data grouped by time windows
    for each dataset's corresponding table in the test database. The benchmark
    runs for a given number of iterations, and the results are saved to a
    SQL table in the results database.

    Arguments:
        test_db: The database instance to benchmark against.
        iterations: The number of times to run the benchmark.
        results_db: The database instance to store the benchmark results.
        datasets: An iterable of dataset objects, where each dataset's name
            corresponds to a table and its `time_column` specifies the column
            for grouping.

    """
    data = defaultdict(list)

    def _run() -> None:
        ds: scoda.datasets.generic.Dataset
        for ds in datasets:
            test_db.query_groupby_time_window_value(
                table_name=ds.name,
                column_name=ds.time_column,
            )

    with Bar(
        "Benchmarking querying time windows per table...",
        max=iterations,
    ) as bar:
        for _ in range(iterations):
            start_time: float = time()
            _run()
            end_time: float = time()
            data["seconds"].append(end_time - start_time)

            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_query_groupby_per_table",
        con=results_db.engine,
        if_exists="append",
        index=False,
    )

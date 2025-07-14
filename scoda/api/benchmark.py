from pandas import DataFrame
from scoda.api.db import DB
from time import time
import scoda.api.dataset as scoda_dataset
from collections.abc import Iterator
from progress.bar import Bar
from collections import defaultdict


def _write(
    db: DB,
    clear_database: bool,
    datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
) -> None:
    if clear_database:
        db.recreate_tables()

    ds: scoda_dataset.Dataset
    for ds in datasets:
        ds.data.to_sql(
            name=ds.name,
            con=db.engine,
            if_exists="append",
            index=False,
        )


def benchmark_total_time_to_batch_write_tables(
    test_db: DB,
    iterations: int,
    benchmark_db: DB,
    datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
) -> None:
    """
    Benchmark the performance of writing multiple datasets to a database by
        measuring the time taken for each iteration and storing the results in a
        benchmark database.

    The function performs the following steps:
    1. Iterates over the specified number of iterations.
    2. For each iteration, writes all datasets to the test database and measures
        the time taken.
    3. Recreates the tables in the test database to ensure a clean state for the
        next iteration.
    4. Collects the time taken for each iteration and stores it in a DataFrame.
    5. Writes the benchmark results to the benchmark database for further
        analysis.

    The progress of the benchmarking process is displayed using a progress bar.

    Args:
        test_db (DB): The database instance where datasets will be written
            during the benchmark.
        iterations (int): The number of times the writing operation should be
            repeated for benchmarking.
        benchmark_db (DB): The database instance where benchmark results will be
            stored.
        datasets (list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset]):
            A collection of datasets to be written to the test database. Can be
            provided as a list or an iterator.
    """
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
        name="benchmark_total_time_to_batch_write_tables",
        con=benchmark_db.engine,
        if_exists="append",
        index=False,
    )


def benchmark_total_time_to_batch_write_individual_tables(
    test_db: DB,
    iterations: int,
    benchmark_db: DB,
    datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
) -> None:
    """
    Benchmarks the performance of writing each dataset individually to a
        database by measuring the time taken for each dataset and storing the
        results in a benchmark database.

    The function performs the following steps:
    1. Iterates over the specified number of iterations.
    2. For each iteration, writes each dataset individually to the test database
        and measures the time taken.
    3. Recreates the tables in the test database to ensure a clean state for the
        next iteration.
    4. Collects the time taken for each dataset and stores it in a DataFrame.
    5. Writes the benchmark results to the benchmark database for further
        analysis.

    The progress of the benchmarking process is displayed using a progress bar.

    Args:
        test_db (DB): The database instance where datasets will be written
            during the benchmark.
        iterations (int): The number of times the writing operation should be
            repeated for benchmarking.
        benchmark_db (DB): The database instance where benchmark results will be
            stored.
        datasets (list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset]):
            A collection of datasets to be written to the test database. Can be
            provided as a list or an iterator.

    """
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
            test_db.recreate_tables()
            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_total_time_to_batch_write_individual_tables",
        con=benchmark_db.engine,
        if_exists="append",
        index=False,
    )


def benchmark_total_time_to_sequential_write_tables(
    test_db: DB,
    iterations: int,
    benchmark_db: DB,
    datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
) -> None:
    """
    Benchmarks the performance of writing all datasets to a database one row at
        a time by measuring the time taken for each iteration and storing the
        results in a benchmark database.

    The function performs the following steps:
    1. Iterates over the specified number of iterations.
    2. For each iteration, writes all datasets to the test database one row at a
        time and measures the time taken.
    3. Recreates the tables in the test database to ensure a clean state for the
        next iteration.
    4. Collects the time taken for each iteration and stores it in a DataFrame.
    5. Writes the benchmark results to the benchmark database for further
        analysis.

    The progress of the benchmarking process is displayed using a progress bar.

    Args:
        test_db (DB): The database instance where datasets will be written
            during the benchmark.
        iterations (int): The number of times the writing operation should be
            repeated for benchmarking.
        benchmark_db (DB): The database instance where benchmark results will be
            stored.
        datasets (list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset]):
            A collection of datasets to be written to the test database. Can be
            provided as a list or an iterator.

    """
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
        name="benchmark_total_time_to_sequential_write_tables",
        con=benchmark_db.engine,
        if_exists="append",
        index=False,
    )


def benchmark_total_time_to_sequential_write_individual_tables(
    test_db: DB,
    iterations: int,
    benchmark_db: DB,
    datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
) -> None:
    """
    Benchmarks the performance of writing each dataset to a database one row at
        a time by measuring the time taken for each dataset and storing the
        results in a benchmark database.

    The function performs the following steps:
    1. Iterates over the specified number of iterations.
    2. For each iteration, writes each dataset to the test database one row at a
        time and measures the time taken.
    3. Recreates the tables in the test database to ensure a clean state for the
        next iteration.
    4. Collects the time taken for each dataset and stores it in a DataFrame.
    5. Writes the benchmark results to the benchmark database for further
        analysis.

    The progress of the benchmarking process is displayed using a progress bar.

    Args:
        test_db (DB): The database instance where datasets will be written
            during the benchmark.
        iterations (int): The number of times the writing operation should be
            repeated for benchmarking.
        benchmark_db (DB): The database instance where benchmark results will be
            stored.
        datasets (list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset]):
            A collection of datasets to be written to the test database. Can be
            provided as a list or an iterator.

    """
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
            test_db.recreate_tables()
            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_total_time_to_sequential_write_individual_tables",
        con=benchmark_db.engine,
        if_exists="append",
        index=False,
    )


def benchmark_min_query_on_each_table(
    test_db: DB,
    clear_db: bool,
    iterations: int,
    benchmark_db: DB,
    datasets: list[scoda_dataset.Dataset] | Iterator[scoda_dataset.Dataset],
) -> None:
    def _run(db: DB, table_name: str) -> None:
        db.query_min_value(table_name=table_name, column_name="measured_kW")

    data: dict[str, list] = defaultdict(list)

    print("Writing data to test database...")
    _write(db=test_db, clear_database=clear_db, datasets=datasets)

    with Bar("Benchmarking minimum value query per table...", max=iterations) as bar:
        for _ in range(iterations):
            dataset: scoda_dataset.Dataset
            start_time: float = time()
            for dataset in datasets:
                _run(db=test_db, table_name=dataset.name)
            end_time: float = time()
            data["seconds"].append(end_time - start_time)
            bar.next()

    df: DataFrame = DataFrame(data=data)
    df.to_sql(
        name="benchmark_min_query_on_each_table",
        con=benchmark_db.engine,
        if_exists="append",
        index=False,
    )


def benchmark_average_query(db: DB) -> DataFrame:
    pass


def benchmark_query(db: str, sql: str) -> DataFrame:
    pass

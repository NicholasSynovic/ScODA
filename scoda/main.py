"""
Main entrypoint to ScODA command line application.

Copyright 2025 (C) Nicholas M. Synovic

"""

from pathlib import Path
from time import time
from typing import Any

import scoda.api.dataset as scoda_dataset
import scoda.benchmarks as scoda_benchmarks
import scoda.db as scoda_db
import scoda.db.document.last as last_doc_db
import scoda.db.relational.last as last_rdbms
import scoda.db.results as scoda_results
from scoda.cli import CLI


def identify_input(key: str) -> bool:
    """
    Identify the dataset type based on the input key.

    Args:
        key (str): The key indicating the dataset type, expected to be in the
            format "dataset_name.some_other_info".

    Returns:
        bool: Returns True if the dataset is LAST, False if it is THETA.

    """
    split_key: list[str] = key.split(sep=".")

    return split_key[0] == "last"


def create_last_db(db_name: str) -> last_rdbms.LAST | last_doc_db.LAST:  # noqa: PLR0911
    """
    Create a database connection on the specified database name.

    Args:
        db_name (str): The name of the database to connect to.

    Returns:
        last_rdbms.LAST | last_doc_db.LAST: An instance of the appropriate
            database connection class.

    """
    match db_name:
        case "couch-db":
            return last_doc_db.CouchDB()
        case "db2":
            return last_rdbms.DB2()
        case "mariadb":
            return last_rdbms.MariaDB()
        case "mysql":
            return last_rdbms.MySQL()
        case "postgres":
            return last_rdbms.PostgreSQL()
        case "sqlite3":
            return last_rdbms.SQLite3(fp=Path(f"{time()}_last.sqlite3"))
        case "sqlite3-memory":
            return last_rdbms.InMemorySQLite3()
        case _:
            return last_rdbms.LAST(
                uri="",
                username="",
                password="",
            )  # nosec: B106


def run_benchmarks(
    test_db: scoda_db.DB,
    results_db: scoda_results.Results,
    datasets: list[scoda_dataset.Dataset],
    iterations: int,
) -> None:
    """
    Run benchmarking tests on the specified database using the provided datasets.

    Performs various benchmarking tests, including batch and sequential writes.

    Args:
        test_db (scoda_db.DB): The database to be tested.
        results_db (scoda_results.Results): The database to store benchmark
            results.
        datasets (list[scoda_dataset.Dataset]): The datasets to be used for
            benchmarking.
        iterations (int): The number of iterations to perform for each benchmark
            test.

    """
    scoda_benchmarks.benchmark_total_time_to_batch_write_tables(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    scoda_benchmarks.benchmark_total_time_to_batch_write_individual_tables(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    scoda_benchmarks.benchmark_total_time_to_sequential_write_tables(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    scoda_benchmarks.benchmark_total_time_to_sequential_write_individual_tables(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )


def main() -> int:
    """
    Execute the benchmarking process.

    Sets up the CLI, identifies the dataset, creates database connections, loads
        datasets, and runs benchmarks.

    Returns:
        int: Status code indicating the result of the execution. Returns 0 for
            success, 1 if Theta dataset is not implemented, 2 if database
            connection fails, and 3 if dataset loading fails.

    """
    cli: CLI = CLI()
    args: dict[str, Any] = cli.parse_args().__dict__
    arg_keys: list[str] = list(args.keys())

    # True if LAST, False if Theta
    print("Identifying dataset...")  # noqa: T201
    dataset: bool = identify_input(key=arg_keys[0])

    # TODO: Implement `theta` benchmarking
    if dataset is False:
        return 1

    # Create db connection
    print("Creating testing database connection...")  # noqa: T201
    test_db: last_rdbms.LAST | last_doc_db.LAST
    test_db = create_last_db(db_name=args["db"][0])
    if test_db.uri is False:
        return 2

    # Create results db connection
    print("Creating benchmarking results database...")  # noqa: T201
    results_db: scoda_results.Results = scoda_results.Results(
        fp=args["output"][0],
    )

    # Load datasets
    print("Loading datasets...")  # noqa: T201
    datasets: list[scoda_dataset.Dataset] | bool
    # TODO: Add support for loading `theta` datasets
    datasets = scoda_dataset.load_llnl_datasets(directory=args["last.input"][0])
    if datasets is False:
        return 3

    # Run benchmarks
    print("Running benchmarks...")  # noqa: T201
    run_benchmarks(
        test_db=test_db,
        results_db=results_db,
        datasets=datasets,
        iterations=args["iterations"][0],
    )

    return 0


if __name__ == "__main__":
    main()

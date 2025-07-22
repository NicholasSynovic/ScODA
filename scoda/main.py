"""
Main entrypoint to ScODA command line application.

Copyright 2025 (C) Nicholas M. Synovic

"""

from typing import Any

import scoda.datasets as scoda_dataset
import scoda.datasets.last as scoda_last_dataset
import scoda.db as scoda_db
import scoda.db.results as scoda_results
from scoda.benchmarks import run_benchmarks
from scoda.cli import CLI
from scoda.utils import create_db_instance, identify_input


def main() -> int:
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
    test_db: scoda_db.DB = create_db_instance(db_name=args["db"][0])
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
    datasets = scoda_last_dataset.load_llnl_datasets(directory=args["last.input"][0])
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

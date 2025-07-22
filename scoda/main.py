"""
Main entrypoint to ScODA command line application.

Copyright 2025 (C) Nicholas M. Synovic

"""

import sys
from collections.abc import Iterable
from typing import Any

import scoda.datasets
import scoda.db as scoda_db
import scoda.db.results as scoda_results
from scoda.benchmarks import run_benchmarks
from scoda.cli import CLI
from scoda.utils import create_db_instance, identify_input


def main() -> int:
    # Handle CLI args
    cli: CLI = CLI()
    args: dict[str, Any] = cli.parse_args().__dict__
    arg_keys: list[str] = list(args.keys())

    # Create results db connection
    results_db: scoda_results.Results = scoda_results.Results(
        fp=args["output"][0],
    )

    # Load the dataset
    dataset_name: str = identify_input(key=arg_keys[0])
    datasets: Iterable[scoda.datasets.Dataset]
    match dataset_name:
        case "last":
            datasets = scoda.datasets.load_llnl_last(directory=args["last.input"][0])
        case "theta":
            datasets = scoda.datasets.load_anl_theta(directory=args["theta.input"][0])
        case _:
            sys.exit(1)

    # Create test db connection
    test_db: scoda_db.DB = create_db_instance(db_name=args["db"][0])
    if test_db.uri is False:
        sys.exit(2)

    # Run benchmarks
    run_benchmarks(
        test_db=test_db,
        results_db=results_db,
        datasets=datasets,
        iterations=args["iterations"][0],
    )

    sys.exit(0)


if __name__ == "__main__":
    main()

"""
Handle command line argument parsing.

Copyright 2025 (C) Nicholas M. Synovic

"""

import importlib.metadata
from argparse import ArgumentParser, Namespace

from scoda.api.utils import resolve_path


def _add_base_arguments(parser: ArgumentParser) -> None:
    parser.add_argument(
        "-o",
        "--output",
        nargs=1,
        type=resolve_path,
        required=True,
        help="Path to SQLite3 database to store benchmark results",
    )
    parser.add_argument(
        "-r",
        "--iterations",
        nargs=1,
        type=int,
        required=True,
        help="Number of iterations to benchmark the database against",
    )


def _configure_relational_dbms(parser: ArgumentParser) -> None:
    parser.add_argument(
        "-d",
        "--db",
        nargs=1,
        type=str,
        choices=[
            "db2",
            "mariadb",
            "mysql",
            "postgres",
            "sqlite3",
            "sqlite3-memory",
        ],
        required=True,
        help="Database to benchmark against",
    )


class CLI:
    def __init__(self) -> None:
        self.parser: ArgumentParser = ArgumentParser(
            prog="ScODA",
            description="Supercomputing Operational Data Analysis DB Benchmarker",
            epilog="Copyright 2025 (C) Nicholas M. Synovic",
        )

        self.parser.add_argument(
            "-v",
            "--version",
            action="version",
            version=importlib.metadata.version(distribution_name="scoda"),
        )

        dataset_subparsers = self.parser.add_subparsers(
            title="Datasets", description="Dataset to benchmark against"
        )
        self.last_subparser: ArgumentParser = dataset_subparsers.add_parser(
            name="last",
            help="Benchmark against the LLNL/LAST dataset",
        )
        self.theta_subparser: ArgumentParser = dataset_subparsers.add_parser(
            name="theta",
            help="Benchmark against the THETA dataset",
        )

        self.configure_last()
        self.configure_theta()

    def configure_last(self) -> None:
        self.last_subparser.add_argument(
            "-i",
            "--input-dir",
            nargs=1,
            type=resolve_path,
            required=True,
            help="Path to LLNL/LAST Power Provisioning Dataset",
            dest="last.input",
        )

        _add_base_arguments(parser=self.last_subparser)
        _configure_relational_dbms(parser=self.last_subparser)

    def configure_theta(self) -> None:
        self.theta_subparser.add_argument(
            "-i",
            "--input-dir",
            nargs=1,
            type=resolve_path,
            required=True,
            help="Path to Theta Dataset",
            dest="theta.input",
        )

        _add_base_arguments(parser=self.theta_subparser)
        _configure_relational_dbms(parser=self.theta_subparser)

    def parse_args(self) -> Namespace:
        """
        Parse the command-line arguments.

        Returns:
            Namespace: The parsed arguments as a Namespace object.

        """
        return self.parser.parse_args()

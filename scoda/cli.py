"""
Handle command line argument parsing.

Copyright 2025 (C) Nicholas M. Synovic

"""

import importlib.metadata
from argparse import ArgumentParser, Namespace

import scoda.utils as scoda_utils


def _add_base_arguments(parser: ArgumentParser) -> None:
    """
    Add base arguments to the argument parser.

    Args:
        parser (ArgumentParser): The argument parser to which the base arguments
            will be added.

    The base arguments include:
        - `--output`: Path to SQLite3 database to store benchmark results.
        - `--iterations`: Number of iterations to benchmark the database against.

    """
    parser.add_argument(
        "-o",
        "--output",
        nargs=1,
        type=scoda_utils.resolve_path,
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


def _configure_dbms(parser: ArgumentParser) -> None:
    """
    Configure the database management system (DBMS) argument for the parser.

    The DBMS argument includes:
        - `--db`: Database to benchmark against.

    Args:
        parser (ArgumentParser): The argument parser to which the DBMS argument
            will be added.

    """
    parser.add_argument(
        "-d",
        "--db",
        nargs=1,
        type=str,
        choices=[
            "couchdb",
            "db2",
            "influxdb",
            "mariadb",
            "mongodb",
            "mysql",
            "postgres",
            "sqlite3",
            "sqlite3-memory",
            "victoriametrics",
            "deltalake",
            "iceberg",
        ],
        required=True,
        help="Database to benchmark against",
    )


class CLI:
    """
    Command Line Interface (CLI) for the ScODA DB Benchmarker.

    This class sets up the argument parser for benchmarking databases against datasets.
    It includes subparsers for specific datasets and configures arguments for each.

    Attributes:
        parser (ArgumentParser): The main argument parser for the CLI.
        last_subparser (ArgumentParser): Subparser for the LLNL/LAST dataset.
        theta_subparser (ArgumentParser): Subparser for the THETA dataset.

    """

    def __init__(self) -> None:
        """
        Initialize the CLI with default settings.

        Sets up the main argument parser and subparsers for datasets.
        Configures version information and dataset-specific arguments.

        """
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
        """
        Configure arguments specific to the LLNL/LAST dataset.

        Adds the input directory argument and base arguments to the LAST subparser.

        """
        self.last_subparser.add_argument(
            "-i",
            "--input-dir",
            nargs=1,
            type=scoda_utils.resolve_path,
            required=True,
            help="Path to LLNL/LAST Power Provisioning Dataset",
            dest="last.input",
        )

        _add_base_arguments(parser=self.last_subparser)
        _configure_dbms(parser=self.last_subparser)

    def configure_theta(self) -> None:
        """
        Configure arguments specific to the THETA dataset.

        Adds the input directory argument and base arguments to the THETA subparser.

        """
        self.theta_subparser.add_argument(
            "-i",
            "--input-dir",
            nargs=1,
            type=scoda_utils.resolve_path,
            required=True,
            help="Path to THETA Dataset",
            dest="theta.input",
        )

        _add_base_arguments(parser=self.theta_subparser)
        _configure_dbms(parser=self.theta_subparser)

    def parse_args(self) -> Namespace:
        """
        Parse the command-line arguments.

        Returns:
            Namespace: The parsed arguments as a Namespace object.

        """
        return self.parser.parse_args()

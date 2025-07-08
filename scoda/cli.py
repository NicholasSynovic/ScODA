"""
Handle command line argument parsing.

Copyright 2025 (C) Nicholas M. Synovic

"""

import importlib.metadata
from argparse import ArgumentParser, Namespace

from scoda.api.utils import resolve_path


class CLI:
    def __init__(self) -> None:
        self.parser: ArgumentParser = ArgumentParser(
            prog="ScODA",
            description="Supercomputing Operational Data Analysis DB Benchmarker",
            epilog="Copyright 2025 (C) Nicholas M. Synovic",
        )
        self.parser.add_argument(
            "-d",
            "--db",
            nargs=1,
            type=str,
            choices=[
                "postgres",
                "mysql",
                "sqlite3",
                "sqlite3-memory",
                "mariadb",
                "db2",
            ],
            required=True,
            help="Database to benchmark against",
        )
        self.parser.add_argument(
            "-i",
            "--input-dir",
            nargs=1,
            type=resolve_path,
            required=True,
            help="Path to LLNL/LAST Power Provisioning Dataset",
        )
        self.parser.add_argument(
            "-o",
            "--output",
            nargs=1,
            type=resolve_path,
            required=True,
            help="Path to SQLite3 database to store benchmark results",
        )
        self.parser.add_argument(
            "-r",
            "--iterations",
            nargs=1,
            type=int,
            required=True,
            help="Number of iterations to benchmark the database against",
        )
        self.parser.add_argument(
            "-v",
            "--version",
            action="version",
            version=importlib.metadata.version(distribution_name="scoda"),
        )

    def parse_args(self) -> Namespace:
        """
        Parse the command-line arguments.

        Returns:
            Namespace: The parsed arguments as a Namespace object.

        """
        return self.parser.parse_args()

"""
Database to store benchmark results.

Copyright 2025 (C) Nicholas M. Synovic

"""

from pathlib import Path

from pandas import DataFrame
from sqlalchemy import Engine, create_engine


class Results:
    """
    SQLite-backed database for storing benchmark results.

    This class provides a simple interface to write pandas DataFrames to a local
    SQLite database file. It is intended for storing and organizing results from
    benchmarking experiments.

    Arguments:
        fp: Path to the SQLite database file. If it does not exist, it will be
            created.

    """

    def __init__(self, fp: Path) -> None:
        """
        Initialize the results database interface.

        This constructor sets up a SQLite engine using the provided file path.

        Arguments:
            fp: Path to the SQLite database file. The path is resolved to an
                absolute path.

        """
        self.fp: Path = fp.resolve()
        self.engine: Engine = create_engine(url=f"sqlite:///{self.fp}")

    def upload(self, data: DataFrame, table_name: str) -> None:
        """
        Upload a pandas DataFrame to the results database.

        This method writes the contents of the given DataFrame to the specified
        table in the SQLite database. If the table already exists, data is
        appended.

        Arguments:
            data: A pandas DataFrame containing benchmark results.
            table_name: The name of the table to upload the data to.

        """
        data.to_sql(
            name=table_name,
            con=self.engine,
            if_exists="append",
            index=False,
        )

"""
Database to store benchmark results.

Copyright 2025 (C) Nicholas M. Synovic

"""

from pandas import DataFrame
from sqlalchemy import Engine, create_engine, MetaData
from pathlib import Path


class Results:
    """
    A class to manage the storage of benchmark results in a SQLite database.

    This class provides methods to initialize a connection to a SQLite database
    and upload data to specified tables within the database.

    Attributes:
        fp (Path): The file path to the SQLite database.
        engine (Engine): The SQLAlchemy engine used for database operations.
        metadata (MetaData): The SQLAlchemy metadata object for schema
            management.

    """

    def __init__(self, fp: Path):
        """
        Initialize the Results instance with a database file path.

        Args:
            fp (Path): The file path to the SQLite database.

        Sets up the SQLAlchemy engine and metadata for database operations.

        """
        self.fp: Path = fp.resolve()

        self.engine: Engine = create_engine(url=f"sqlite:///{self.fp}")
        self.metadata: MetaData = MetaData()

    def upload(self, data: DataFrame, table_name: str) -> None:
        """
        Upload data to a specified table in the SQLite database.

        Uses pandas' to_sql method to append the data to the specified table.

        Args:
            data (DataFrame): The data to be uploaded, represented as a pandas
                DataFrame.
            table_name (str): The name of the table to which the data will be
                appended.

        """
        data.to_sql(
            name=table_name,
            con=self.engine,
            if_exists="append",
            index=False,
        )

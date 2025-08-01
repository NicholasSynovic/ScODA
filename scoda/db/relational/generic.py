"""
Generic relational database.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from abc import abstractmethod
from collections.abc import Iterable

import pandas as pd
from sqlalchemy import (
    Engine,
    MetaData,
    Table,
    create_engine,
    func,
    select,
)

import scoda.datasets.generic
import scoda.db


class RelationalDB(scoda.db.DB):
    """
    Abstract base class for relational database backends using SQLAlchemy.

    This class provides core functionality for initializing a connection to
    a relational database and managing metadata. Subclasses should extend
    this class to implement database-specific behavior.

    Attributes:
        engine: SQLAlchemy Engine used for database connections.
        metadata: SQLAlchemy MetaData object for reflecting and manipulating schema.

    Args:
        connection_string: SQLAlchemy connection string for the target database.
        convert_time_column_to_int: Whether to convert time columns to integers.

    """

    def __init__(
        self,
        connection_string: str,
        convert_time_column_to_int: bool = False,  # noqa: FBT001, FBT002
    ) -> None:
        """
        Initialize a relational database connection using SQLAlchemy.

        Args:
            connection_string: A SQLAlchemy-compatible connection string
                to connect to the relational database (e.g., SQLite, MySQL, PostgreSQL).
            convert_time_column_to_int: Whether to convert time columns to integer
                representations for internal processing (default is False).

        """
        super().__init__(convert_time_column_to_int=convert_time_column_to_int)
        self.engine: Engine = create_engine(url=connection_string)
        self.metadata: MetaData = MetaData()

    def batch_upload(self, dataset: scoda.datasets.generic.Dataset) -> None:
        """
        Upload data in batch to the database.

        This method iterates over a dataset and sends each record to the
        database.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` instance.

        """
        dataset.data.to_sql(
            name=dataset.name,
            con=self.engine,
            if_exists="append",
            index=False,
        )

    def batch_read(self, table_name: str) -> None:
        """
        Perform a batch read of all metrics from the database.

        Arguments:
            table_name: The name of the table to read from.

        """
        pd.read_sql_table(table_name=table_name, con=self.engine)

    def create(self) -> None:  # noqa: PLR6301
        """Create the database if it does not already exist."""
        return None  # noqa: RET501

    def delete(self) -> None:
        """
        Delete all data from the database.

        This method sends removes all stored data.

        """
        self.metadata.reflect(bind=self.engine)
        self.metadata.drop_all(bind=self.engine)

    def query_average_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query the average value of a column in a given table.

        This method retrieves the average value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the average value.

        """
        table: Table = Table(
            table_name,
            self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            query = select(func.avg(table.c[column_name]))
            connection.execute(query)
            connection.close()

    @abstractmethod
    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query a time window value grouped by a table name and column name.

        This function sends a request to group a specified column over an one
        hour time window.

        Arguments:
            table_name: The name of the table to group by.
            column_name: The name of the column to average over time.

        """
        ...

    def query_max_value(self, table_name: str, column_name: str) -> None:
        """
        Query the maximum value of a column in a given table.

        This method retrieves the maximum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the maximum value.

        """
        table: Table = Table(
            table_name,
            self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            query = select(func.max(table.c[column_name]))
            connection.execute(query)
            connection.close()

    def query_min_value(self, table_name: str, column_name: str) -> None:
        """
        Query the minimum value of a column in a given table.

        This method retrieves the minimum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the minimum value.

        """
        table: Table = Table(
            table_name,
            self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            query = select(func.min(table.c[column_name]))
            connection.execute(query)
            connection.close()

    @abstractmethod
    def query_mode_value(self, table_name: str, column_name: str) -> None:
        """
        Query the mode value from a specified table and column.

        It is intended to support analysis for determining the most frequent
        (mode) value within the specified table and column.

        Arguments:
            table_name: The name of the table to query.
            column_name: The name of the column to extract the mode value from.

        """
        ...

    def sequential_read(self, table_name: str, rows: int) -> None:
        """
        Perform a sequential read of records from a specified table.

        This method constructs and executes a  query to read all records from
        the specified table. It counts the number of records streamed from the
        query result but does not return or store them.

        Arguments:
            table_name: The name of the table (i.e., measurement) to read from.
            rows: The number of rows to read.

        """
        dfs: Iterable[pd.DataFrame] = pd.read_sql_table(
            table_name=table_name,
            con=self.engine,
            chunksize=rows,
        )

        df: pd.DataFrame
        for df in dfs:
            df.isna()

    def sequential_upload(
        self,
        dataset: scoda.datasets.generic.Dataset,
    ) -> None:
        """
        Upload data sequentially to the database.

        This method iterates over each row in a dataset and writes it
        individually to the database with a short delay between uploads. Each
        row is converted to a line protocol point with appropriate tags and
        fields. Invalid or missing field values are skipped, and non-numeric
        strings are cast to float where possible.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` containing time series data
                    with a time index and one or more value columns.

        """
        dataset.data.to_sql(
            name=dataset.name,
            con=self.engine,
            if_exists="append",
            index=False,
            chunksize=1,
        )

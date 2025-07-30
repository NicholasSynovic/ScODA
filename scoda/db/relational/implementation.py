"""
Implementations of relational databases.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import os
from pathlib import Path

from sqlalchemy import Table, desc, func, select

from scoda.db.relational.generic import RelationalDB


class PostgreSQL(RelationalDB):
    """
    PostgreSQL connection.

    Inherits from `RelationalDB`.
    """

    def __init__(self) -> None:
        """
        Initialize a PostgreSQL connection with default credentials.

        Connects to a local PostgreSQL instance for the 'research' database.

        """
        username: str = os.environ["POSTGRESQL_USERNAME"]
        password: str = os.environ["POSTGRESQL_PASSWORD"]
        uri: str = os.environ["POSTGRESQL_URI"]
        database: str = os.environ["POSTGRESQL_DATABASE"]
        super().__init__(
            connection_string=f"postgresql+psycopg2://{username}:{password}@{uri}/{database}",
            convert_time_column_to_int=True,
        )

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
        table: Table = Table(
            table_name,
            self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            query = (
                select(
                    func.date_trunc(
                        "hour", func.to_timestamp(table.c[column_name])
                    ).label("hour"),
                    func.avg(table.c[column_name]).label("average_value"),
                )
                .group_by("hour")
                .order_by("hour")
            )
            connection.execute(query)
            connection.close()

    def query_mode_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query the mode value from a specified table and column.

        It is intended to support analysis for determining the most frequent
        (mode) value within the specified table and column.

        Arguments:
            table_name: The name of the table to query.
            column_name: The name of the column to extract the mode value from.

        """
        table: Table = Table(
            table_name,
            self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            query = select(func.mode().within_group(table.c[column_name]))
            connection.execute(query)
            connection.close()


class GenericMySQL(RelationalDB):
    """
    A generic MySQL-compatible relational database implementation.

    This class serves as a base for specific MySQL database configurations,
    such as MariaDB and MySQL itself. It disables time column conversion
    to integer format to preserve native datetime handling.

    Arguments:
        connection_string: SQLAlchemy-compatible connection URI for MySQL.

    """

    def __init__(self, connection_string: str) -> None:
        """
        Initialize a generic relational database connection.

        Args:
            connection_string: A SQLAlchemy-compatible connection string
                for the target relational database.

        """
        super().__init__(
            connection_string=connection_string,
            convert_time_column_to_int=False,
        )

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
        table: Table = Table(
            table_name,
            self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            query = (
                select(
                    func.date_format(table.c[column_name], "%Y-%m-%d %H:00:00").label(
                        "hour"
                    ),
                    func.avg(table.c[column_name]).label("average_value"),
                )
                .group_by("hour")
                .order_by("hour")
            )
            connection.execute(query)
            connection.close()

    def query_mode_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query the mode value from a specified table and column.

        It is intended to support analysis for determining the most frequent
        (mode) value within the specified table and column.

        Arguments:
            table_name: The name of the table to query.
            column_name: The name of the column to extract the mode value from.

        """
        table: Table = Table(
            table_name,
            self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            subquery = (
                select(
                    table.c[column_name],
                    func.count(table.c[column_name]).label("count"),
                )
                .group_by(table.c[column_name])
                .subquery()
            )

            query = (
                select(subquery.c[column_name])
                .order_by(desc(subquery.c.count))
                .limit(1)
            )
            connection.execute(query)
            connection.close()


class MariaDB(GenericMySQL):
    """
    MariaDB connection.

    Inherits from `GenericMySQL`.
    """

    def __init__(self) -> None:
        """
        Initialize a MariaDB connection with default credentials.

        Uses PyMySQL to connect to a local MariaDB instance for the 'research'
        database.

        """
        username: str = os.environ["MARIADB_USERNAME"]
        password: str = os.environ["MARIADB_PASSWORD"]
        uri: str = os.environ["MARIADB_URI"]
        database: str = os.environ["MARIADB_DATABASE"]
        super().__init__(
            connection_string=f"mariadb+pymysql://{username}:{password}@{uri}/{database}",
        )


class MySQL(GenericMySQL):
    """
    MySQL connection.

    Inherits from `GenericMySQL`.
    """

    def __init__(self) -> None:
        """
        Initialize a MySQL connection with default credentials.

        Uses PyMySQL to connect to a local MySQL instance for the 'research'
        database.

        """
        username: str = os.environ["MYSQL_USERNAME"]
        password: str = os.environ["MYSQL_PASSWORD"]
        uri: str = os.environ["MYSQL_URI"]
        database: str = os.environ["MYSQL_DATABASE"]
        super().__init__(
            connection_string=f"mysql+pymysql://{username}:{password}@{uri}/{database}",
        )


class GenericSQLite3(RelationalDB):
    """
    Base SQLite3 database interface.

    This class extends `RelationalDB` and allows initialization of a SQLite database
    using any valid SQLAlchemy SQLite connection string.

    Attributes:
        connection_string (str): SQLAlchemy connection string for the SQLite database.

    """

    def __init__(self, connection_string: str) -> None:
        """
        Initialize a generic SQLite3 database.

        This constructor sets up a SQLite connection with the provided connection string
        and disables conversion of time columns to integers.

        Arguments:
            connection_string: SQLAlchemy connection string for the SQLite database.

        """
        super().__init__(
            connection_string=connection_string,
            convert_time_column_to_int=False,
        )

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
        table: Table = Table(
            table_name,
            self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            query = (
                select(
                    func.strftime("%Y-%m-%d %H:00:00", table.c[column_name]).label(
                        "hour"
                    ),
                    func.avg(table.c[column_name]).label("average_value"),
                )
                .group_by("hour")
                .order_by("hour")
            )
            connection.execute(query)
            connection.close()

    def query_mode_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query the mode value from a specified table and column.

        It is intended to support analysis for determining the most frequent
        (mode) value within the specified table and column.

        Arguments:
            table_name: The name of the table to query.
            column_name: The name of the column to extract the mode value from.

        """
        table: Table = Table(
            table_name,
            self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            subquery = (
                select(
                    table.c[column_name],
                    func.count(table.c[column_name]).label("count"),
                )
                .group_by(table.c[column_name])
                .subquery()
            )

            query = (
                select(subquery.c[column_name])
                .order_by(desc(subquery.c.count))
                .limit(1)
            )
            connection.execute(query)
            connection.close()


class InMemorySQLite3(GenericSQLite3):
    """
    SQLite3 database stored entirely in memory.

    This subclass of `GenericSQLite3` initializes a temporary in-memory database
    that is useful for testing or ephemeral data processing tasks.

    """

    def __init__(self) -> None:
        """
        Initialize an in-memory SQLite3 database.

        This constructor sets the SQLite connection string to `sqlite:///:memory:`,
        which creates a temporary database that resides only in system memory.

        """
        super().__init__(connection_string="sqlite:///:memory:")


class SQLite3(GenericSQLite3):
    """
    SQLite3 database backed by a file on disk.

    This subclass of `GenericSQLite3` initializes a SQLite database using a
    file-based connection, storing all data persistently at the given path.

    Arguments:
        fp: Path to the SQLite database file.

    """

    def __init__(self, fp: Path) -> None:
        """
        Initialize a file-backed SQLite3 database.

        This constructor resolves the given file path and sets up a connection
        to a SQLite database located at that path.

        Arguments:
            fp: Path to the SQLite database file. Will be resolved to an
                absolute path.

        """
        self.fp: Path = fp.resolve()
        super().__init__(connection_string=f"sqlite:///{self.fp}")

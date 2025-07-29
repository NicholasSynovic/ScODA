from pathlib import Path

from sqlalchemy import Table, desc, func, select

from scoda.db.relational.generic import RelationalDB


class PostgreSQL(RelationalDB):
    def __init__(self) -> None:
        super().__init__(
            connection_string="postgresql+psycopg2://admin:example@localhost:5432/research",
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
    def __init__(self, connection_string: str) -> None:
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


class MariaDB(GenericMySQL):
    def __init__(self) -> None:
        super().__init__(
            connection_string="mariadb+pymysql://root:example@localhost:3306/research",
        )


class MySQL(GenericMySQL):
    def __init__(self) -> None:
        super().__init__(
            connection_string="mysql+pymysql://root:example@localhost:3307/research",
        )


class GenericSQLite3(RelationalDB):
    def __init__(self, connection_string: str) -> None:
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
    def __init__(self) -> None:
        super().__init__(connection_string="sqlite:///:memory:")


class SQLite3(GenericSQLite3):
    def __init__(self, fp: Path) -> None:
        self.fp: Path = fp.resolve()
        super().__init__(connection_string=f"sqlite:///{self.fp}")

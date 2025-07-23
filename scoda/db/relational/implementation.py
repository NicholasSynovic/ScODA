from pathlib import Path

from sqlalchemy import Table, desc, func, select

from scoda.db.relational.generic import RelationalDB


class MariaDB(RelationalDB):
    def __init__(self) -> None:
        super().__init__(
            connection_string="mariadb+pymysql://root:example@localhost:3306/research",
        )

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_mode_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...


class MySQL(RelationalDB):
    def __init__(self) -> None:
        super().__init__(
            connection_string="mysql+pymysql://root:example@localhost:3307/research"
        )

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_mode_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...


class PostgreSQL(RelationalDB):
    def __init__(self) -> None:
        super().__init__(
            connection_string="postgresql+psycopg2://admin:example@localhost:5432/research"
        )

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_mode_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...


class GenericSQLite3(RelationalDB):
    def __init__(self, connection_string: str) -> None:
        super().__init__(connection_string=connection_string)

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
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

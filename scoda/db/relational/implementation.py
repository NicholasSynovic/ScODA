from pathlib import Path

from sqlalchemy import Table, func, select

from scoda.db.relational.generic import Relational


class MariaDB(Relational):
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


class MySQLDB(Relational):
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


class PostgreSQL(Relational):
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


class GenericSQLite3(Relational):
    def __init__(self, connection_string: str) -> None:
        super().__init__(connection_string=connection_string)

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

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
            query = select(func.mode(table.c[column_name]))
            connection.execute(query)
            connection.close()
            # try:
            #     connection.execute(query)
            # except (OperationalError, ProgrammingError):
            #     subquery = (
            #         select(
            #             table.c[column_name],
            #             func.count(table.c[column_name]).label("count"),
            #         )
            #         .group_by(table.c[column_name])
            #         .subquery()
            #     )

            #     query = (
            #         select(subquery.c[column_name])
            #         .order_by(desc(subquery.c.count))
            #         .limit(1)
            #     )


class InMemorySQLite3(GenericSQLite3):
    def __init__(self) -> None:
        super().__init__(connection_string="sqlite:///:memory:")


class SQLite3(GenericSQLite3):
    def __init__(self, fp: Path) -> None:
        self.fp: Path = fp.resolve()
        super().__init__(connection_string=f"sqlite:///{self.fp}")

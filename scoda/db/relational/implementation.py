from pathlib import Path

from scoda.db.relational.generic import Relational


class MariaDB(Relational):
    def __init__(self) -> None:
        super().__init__(
            connection_string="mariadb+pymysql://root:example@localhost:3306/research",
        )

    def query_average_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_max_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_min_value(
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

    def query_average_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_max_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_min_value(
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

    def query_average_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_max_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_min_value(
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
        super().__init__(connection_string)

    def query_average_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_max_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_min_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_mode_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...


class InMemorySQLite3(GenericSQLite3):
    def __init__(self) -> None:
        super().__init__(connection_string="sqlite:///:memory:")


class SQLite3(GenericSQLite3):
    def __init__(self, fp: Path) -> None:
        self.fp: Path = fp.resolve()
        super().__init__(connection_string=f"sqlite:///{self.fp}")

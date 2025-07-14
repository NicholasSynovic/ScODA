from abc import ABC
from sqlalchemy import Engine, create_engine, MetaData, select, func, Table
from abc import abstractmethod
from typing import Any
from requests import Response


class DB(ABC):
    def __init__(self, uri: str) -> None:
        self.uri: str = uri
        self.engine: Engine = create_engine(url=self.uri)
        self.metadata: MetaData = MetaData()

        self.create_tables()

    def recreate_tables(self) -> None:
        self.metadata.reflect(bind=self.engine)
        self.metadata.drop_all(bind=self.engine)
        self.metadata.create_all(bind=self.engine, checkfirst=True)

    def query_min_value(self, table_name: str, column_name: str) -> Any:
        table: Table = Table(
            table_name,
            self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            minimum_value_query = select(func.min(table.c[column_name]))
            result = connection.execute(minimum_value_query)
            minimum_value = result.scalar()

        return minimum_value

    def query_avg_value(self, table_name: str, column_name: str) -> Any:
        table: Table = Table(
            table_name,
            self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            average_value_query = select(func.avg(table.c[column_name]))
            result = connection.execute(average_value_query)
            average_value = result.scalar()

        return average_value

    @abstractmethod
    def create_tables(self) -> None: ...


class DocumentDB(ABC):
    def __init__(self, url: str, username: str, password: str) -> None:
        self.url = url
        self.username: str = username
        self.password: str = password
        self.database_name: str = "research"

    @abstractmethod
    def create_database(self) -> None: ...

    @abstractmethod
    def upload(self, data: str) -> Response: ...

    @abstractmethod
    def query_avg_value(self) -> None: ...

    @abstractmethod
    def query_min_value(self) -> None: ...

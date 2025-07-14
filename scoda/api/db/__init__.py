from abc import ABC
from sqlalchemy import Engine, create_engine, MetaData, select, func, Table
from abc import abstractmethod
from typing import Any


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
            metadata=self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            minimum_value_query = select(func.min(table.c[column_name]))
            result = connection.execute(minimum_value_query)
            minimum_value = result.scalar()

        return minimum_value

    @abstractmethod
    def create_tables(self) -> None: ...

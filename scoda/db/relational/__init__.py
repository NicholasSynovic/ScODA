import scoda.db as scoda_db
from sqlalchemy import (
    Engine,
    create_engine,
    MetaData,
    Table,
    select,
    func,
)
from typing import Any
import scoda.api.dataset as scoda_dataset
from abc import abstractmethod


class RDBMS(scoda_db.DB):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str = "research",
    ):
        super().__init__(uri, username, password, database)
        self.engine: Engine = create_engine(url=self.uri)
        self.metadata: MetaData = MetaData()

        self.create()

    @abstractmethod
    def create(self) -> None: ...

    def recreate(self) -> None:
        self.metadata.reflect(bind=self.engine)
        self.metadata.drop_all(bind=self.engine)
        self.metadata.create_all(bind=self.engine, checkfirst=True)

    def batch_upload(self, data: scoda_dataset.Dataset) -> None:
        data.data.to_sql(
            name=data.name,
            con=self.engine,
            if_exists="append",
            index=False,
        )

    def sequential_upload(self, data: scoda_dataset.Dataset) -> None:
        data.data.to_sql(
            name=data.name,
            con=self.engine,
            if_exists="append",
            index=False,
            chunksize=1,
        )

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

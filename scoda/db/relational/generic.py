from abc import abstractmethod
from collections.abc import Iterable

import pandas as pd
from sqlalchemy import Engine, MetaData, create_engine

import scoda.datasets
import scoda.db


class Relational(scoda.db.DB):
    def __init__(self, connection_string: str) -> None:
        super().__init__()
        self.engine: Engine = create_engine(url=connection_string)
        self.metadata: MetaData = MetaData()

    def batch_upload(self, dataset: scoda.datasets.Dataset) -> None:
        dataset.data.to_sql(
            name=dataset.name,
            con=self.engine,
            if_exists="append",
            index=False,
        )

    def batch_read(self, table_name: str) -> None:
        pd.read_sql_table(table_name=table_name, con=self.engine)

    def create(self) -> None:
        pass

    @abstractmethod
    def query_average_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    @abstractmethod
    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    @abstractmethod
    def query_max_value(self, table_name: str, column_name: str) -> None: ...

    @abstractmethod
    def query_min_value(self, table_name: str, column_name: str) -> None: ...

    @abstractmethod
    def query_mode_value(self, table_name: str, column_name: str) -> None: ...

    def recreate(self) -> None:
        self.metadata.reflect(bind=self.engine)
        self.metadata.drop_all(bind=self.engine)
        self.create()

    def sequential_read(self, table_name: str, rows: int) -> None:
        dfs: Iterable[pd.DataFrame] = pd.read_sql_table(
            table_name=table_name,
            con=self.engine,
            chunksize=rows,
        )

        df: pd.DataFrame
        for df in dfs:
            df.isna()

    def sequential_upload(self, dataset: scoda.datasets.Dataset) -> None:
        dataset.data.to_sql(
            name=dataset.name,
            con=self.engine,
            if_exists="append",
            index=False,
            chunksize=1,
        )

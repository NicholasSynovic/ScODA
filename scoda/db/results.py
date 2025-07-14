import scoda.db as scoda_db
from pandas import DataFrame
from sqlalchemy import Engine, create_engine, MetaData
from pathlib import Path
from typing import Any


class Results(scoda_db.DB):
    def __init__(self, fp: Path):
        self.fp: Path = fp.resolve()
        super().__init__(
            uri=f"sqlite:///{self.fp}",
            username="",
            password="",
            database="",
        )
        self.engine: Engine = create_engine(url=self.uri)
        self.metadata: MetaData = MetaData()

    def create(self) -> None:
        pass

    def recreate(self) -> None:
        pass

    def batch_upload(self, data: Any) -> None:
        pass

    def sequential_upload(self, data: Any) -> None:
        pass

    def upload(self, data: DataFrame, table_name: str) -> None:
        data.to_sql(
            name=table_name,
            con=self.engine,
            if_exists="append",
            index=False,
        )

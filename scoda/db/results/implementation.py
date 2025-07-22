"""
Database to store benchmark results.

Copyright 2025 (C) Nicholas M. Synovic

"""

from pathlib import Path

from pandas import DataFrame
from sqlalchemy import Engine, create_engine


class Results:
    def __init__(self, fp: Path) -> None:
        self.fp: Path = fp.resolve()
        self.engine: Engine = create_engine(url=f"sqlite:///{self.fp}")

    def upload(self, data: DataFrame, table_name: str) -> None:
        data.to_sql(
            name=table_name,
            con=self.engine,
            if_exists="append",
            index=False,
        )

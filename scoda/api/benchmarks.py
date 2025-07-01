from pandas import DataFrame
from scoda.api.db import DB
from collections import defaultdict
from time import time


def benchmark_entire_db_write(
    db: DB,
    db_name: str,
    dfs: dict[str, DataFrame],
    iterations: int,
) -> DataFrame:
    def _run() -> None:
        name: str
        df: DataFrame
        for name, df in dfs.items():
            df.to_sql(name=name, con=db.engine, if_exists="append", index=False)

    data: dict[str, list[float]] = defaultdict(list)
    for _ in range(iterations):
        start_time: float = time()
        _run()
        end_time: float = time()
        data[db_name].append(end_time - start_time)

    return DataFrame(data=data)


def benchmark_per_db_table_write(
    db: DB,
    db_name: str,
    dfs: dict[str, DataFrame],
    iterations: int,
) -> DataFrame:
    def _run(name: str, df: DataFrame) -> float:
        start_time: float = time()
        df.to_sql(name=name, con=db.engine, if_exists="append", index=False)
        end_time: float = time()
        return end_time - start_time

    data: dict[str, list[float]] = defaultdict(list)
    for _ in range(iterations):
        name: str
        df: DataFrame
        for name, df in dfs.items():
            key: str = f"{name}_{db_name}"
            data[key].append(_run(name=name, df=df))

    return DataFrame(data=data)

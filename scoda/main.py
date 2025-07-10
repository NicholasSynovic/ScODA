from scoda.cli import CLI
from scoda.api.db import implementations, llnl_last, theta
import scoda.api.dataset as scoda_dataset
from pathlib import Path
from scoda.api.benchmark import *
from collections import defaultdict
from progress.bar import Bar
from time import time


def create_db(db_name: str) -> llnl_last.LLNL_LAST | theta.Theta | bool:
    match db_name:
        case "postgres":
            return implementations.PostgreSQL()
        case "postgres-theta":
            return implementations.PostgreSQL_Theta()
        case "mysql":
            return implementations.MySQL()
        case "sqlite3":
            return implementations.SQLite3(fp=Path(f"{time()}.sqlite3"))
        case "sqlite3-memory":
            return implementations.InMemorySQLite3()
        case "mariadb":
            return implementations.MariaDB()
        case "db2":
            return implementations.DB2()
        case _:
            return False


def read_datasets(directory: Path) -> list[scoda_dataset.Dataset] | bool:
    data: list[scoda_dataset.Dataset] = []

    try:
        data.append(scoda_dataset.CoriPower(directory=directory))
        data.append(scoda_dataset.HawkPower(directory=directory))
        data.append(scoda_dataset.HPCGDPC(directory=directory))
        data.append(scoda_dataset.HPCGSPC(directory=directory))
        data.append(scoda_dataset.HPCGUC(directory=directory))
        data.append(scoda_dataset.HPLDPC(directory=directory))
        data.append(scoda_dataset.HPLSPC(directory=directory))
        data.append(scoda_dataset.HPLUC(directory=directory))
        data.append(scoda_dataset.LumiHPCG(directory=directory))
        data.append(scoda_dataset.LumiPower(directory=directory))
        data.append(scoda_dataset.Marconi100Power(directory=directory))
        data.append(scoda_dataset.PerlmutterPower(directory=directory))
    except FileNotFoundError:
        return False

    return data


def benchmark_db(
    iterations: int,
    db: llnl_last.DB,
    datasets: list[scoda_dataset.Dataset],
    benchmark_results_db: implementations.BenchmarkResults,
) -> None:
    # Write all tables to the DB
    data: dict[str, list[float]] = defaultdict(list)
    with Bar("Benchmarking writing all tables to DB...", max=iterations) as bar:
        for _ in range(iterations):
            data["seconds"].append(
                benchmark_write_all_tables(
                    db=db,
                    datasets=datasets,
                )
            )
            bar.next()
    results: DataFrame = DataFrame(data=data)
    results.to_sql(
        name="benchmark_write_all_tables",
        con=benchmark_results_db.engine,
        if_exists="append",
        index=False,
    )

    # Benchmark per table writes
    data: dict[str, list[float]] = defaultdict(list)
    with Bar("Benchmarking per table writes to DB...", max=iterations) as bar:
        dataset: scoda_dataset.Dataset
        for _ in range(iterations):
            for dataset in datasets:
                data[f"{dataset.name}_seconds"].append(
                    benchmark_per_db_table_write(db=db, dataset=dataset),
                )
            bar.next()

    results: DataFrame = DataFrame(data=data)
    results.to_sql(
        name="benchmark_per_table_write",
        con=benchmark_results_db.engine,
        if_exists="append",
        index=False,
    )


def main() -> int:
    cli: CLI = CLI()
    args = cli.parse_args().__dict__

    # 0: Connect to database
    db: llnl_last.DB | bool = create_db(db_name=args["db"][0])
    if isinstance(db, bool):
        return 1

    # Clear tables even if there is nothing in them
    db.recreate_tables()

    # 1: Connect to benchmark result DB
    benchmark_result_db: implementations.BenchmarkResults = (
        implementations.BenchmarkResults(fp=args["output"][0])
    )

    # 2. Read datasets into memory
    datasets: list[scoda_dataset.Dataset] | bool = read_datasets(
        directory=args["input_dir"][0]
    )
    if isinstance(datasets, bool):
        return 2

    # 3. Benchmark writing to database
    benchmark_db(
        iterations=args["iterations"][0],
        db=db,
        datasets=datasets,
        benchmark_results_db=benchmark_result_db,
    )

    return 0


if __name__ == "__main__":
    main()

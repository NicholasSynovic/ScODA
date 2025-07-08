from scoda.cli import CLI
import scoda.api.db as scoda_db
import scoda.api.dataset as scoda_dataset
from pathlib import Path
from scoda.api.benchmark import *
from collections import defaultdict
from progress.bar import Bar
from time import time


def create_db(db_name: str) -> scoda_db.DB | bool:
    match db_name:
        case "postgres":
            return scoda_db.PostgreSQL()
        case "mysql":
            return scoda_db.MySQL()
        case "sqlite3":
            return scoda_db.SQLite3(fp=Path(f"{time()}.sqlite3"))
        case "sqlite3-memory":
            return scoda_db.InMemorySQLite3()
        case "mariadb":
            return scoda_db.MariaDB()
        case "db2":
            return scoda_db.DB2()
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
    db: scoda_db.DB,
    datasets: list[scoda_dataset.Dataset],
    benchmark_results_db: scoda_db.BenchmarkResults,
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
    db: scoda_db.DB | bool = create_db(db_name=args["db"][0])
    if isinstance(db, bool):
        return 1

    # Clear tables even if there is nothing in them
    db.recreate_tables()

    # 1: Connect to benchmark result DB
    benchmark_result_db: scoda_db.BenchmarkResults = scoda_db.BenchmarkResults(
        fp=args["output"][0]
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

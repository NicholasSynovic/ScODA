import scoda.db.relational.last as last_db
from scoda.db.relational import RDBMS
import scoda.db.benchmark as scoda_benchmark
from typing import Literal
from pathlib import Path
from time import time
from scoda.cli import CLI
from typing import Any


def identify_input(key: str) -> bool:
    # Returns True if LAST dataset, False if Theta
    split_key: list[str] = key.split(sep=".")

    if split_key[0] == "last":
        return True
    else:
        return False


def create_last_db(db_name) -> last_db.LAST:
    match db_name:
        case "db2":
            return last_db.DB2()
        case "mariadb":
            return last_db.MariaDB()
        case "mysql":
            return last_db.MySQL()
        case "postgres":
            return last_db.PostgreSQL()
        case "sqlite3":
            return last_db.SQLite3(fp=Path(f"{time()}_last.sqlite3"))
        case "sqlite3-memory":
            return last_db.InMemorySQLite3()
        case _:
            return last_db.LAST(uri="", username="", password="")


def main() -> int:
    cli: CLI = CLI()
    args: dict[str, Any] = cli.parse_args().__dict__
    arg_keys: list[str] = list(args.keys())

    # True if LAST, False if Theta
    print("Identifying dataset...")
    dataset: bool = identify_input(key=arg_keys[0])

    # TODO: Implement `theta` benchmarking
    if dataset is False:
        return 1

    # Create db connection
    print("Creating testing database connection...")
    test_db: last_db.LAST
    test_db = create_last_db(db_name=args["db"][0])
    if test_db.uri == "":
        return 2

    # Create benchmark db connection
    print("Creating benchmarking results database...")
    benchmark_db: scoda_benchmark.Benchmark = scoda_benchmark.Benchmark(
        fp=args["output"][0],
    )

    # Load datasets
    datasets: list[scoda_dataset.Dataset] | bool

    datasets: list[scoda_dataset.Dataset] | bool = None
    # 1. Identify what dataset type was choosen
    if issubclass(db.__class__, llnl_last.LLNL_LAST):
        dataset_type = "llnl"
        # Connect to LLNL/LAST benchmark result DB
        benchmark_result_db = implementations.BenchmarkResults_LLNL(
            fp=args["output"][0]
        )

        # Load LLNL/LAST datasets
        datasets: list[scoda_dataset.Dataset] | bool = scoda_dataset.load_llnl_datasets(
            directory=args["input_dir"][0],
        )

        # Check if dataset loading failed
        if isinstance(datasets, bool):
            return 2
    else:
        return 1
        dataset_type = "theta"
        # Connect to THETA benchmark result DB
        benchmark_result_db = implementations.BenchmarkResults_Theta(
            fp=args["output"][0]
        )

    # 3. Benchmark writing to database
    if dataset_type == "llnl":
        benchmark_db_llnl(
            iterations=args["iterations"][0],
            db=db,
            datasets=datasets,
            benchmark_results_db=benchmark_result_db,
        )
    else:
        # benchmark_db_theta(
        #     iterations=args["iterations"][0],
        #     db=db,
        #     directory=args["input_dir"][0],
        #     benchmark_results_db=benchmark_result_db,
        # )
        return 2

    return 0


if __name__ == "__main__":
    main()

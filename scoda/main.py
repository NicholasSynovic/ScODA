import scoda.db.relational.last as last_db
import scoda.db.results as scoda_results
from pathlib import Path
from time import time
from scoda.cli import CLI
from typing import Any
import scoda.api.dataset as scoda_dataset
import scoda.db as scoda_db


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


def run_benchmarks(
    test_db: scoda_db.DB,
    results_db: scoda_results.Results,
    datasets: list[scoda_dataset.Dataset],
) -> None:
    pass


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

    # Create results db connection
    print("Creating benchmarking results database...")
    results_db: scoda_results.Results = scoda_results.Results(
        fp=args["output"][0],
    )

    # Load datasets
    print("Loading datasets...")
    datasets: list[scoda_dataset.Dataset] | bool
    # TODO: Add support for loading `theta` datasets
    datasets = scoda_dataset.load_llnl_datasets(directory=args["input_dir"][0])
    if datasets is False:
        return 3

    # Run benchmarks
    print("Running benchmarks...")
    run_benchmarks(test_db=test_db, results_db=results_db, datasets=datasets)

    return 0


if __name__ == "__main__":
    main()

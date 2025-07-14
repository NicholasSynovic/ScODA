from scoda.cli import CLI
from scoda.api.db import implementations, llnl_last, theta
import scoda.api.dataset as scoda_dataset
from pathlib import Path
from scoda.api.benchmark import *
from collections import defaultdict
from progress.bar import Bar
from time import time
from collections.abc import Iterator
from copy import copy
from typing import Literal


def create_db(db_name: str) -> llnl_last.LLNL_LAST | theta.Theta | bool:
    match db_name:
        case "postgres-theta":
            return implementations.PostgreSQL_Theta()
        case "postgres-llnl":
            return implementations.PostgreSQL_LLNL()
        case "mysql-theta":
            return implementations.MySQL_Theta()
        case "mysql-llnl":
            return implementations.MySQL_LLNL()
        case "sqlite3-theta":
            return implementations.SQLite3_Theta(fp=Path(f"{time()}_theta.sqlite3"))
        case "sqlite3-llnl":
            return implementations.SQLite3_LLNL(fp=Path(f"{time()}_llnl.sqlite3"))
        case "sqlite3-memory-theta":
            return implementations.InMemorySQLite3_Theta()
        case "sqlite3-memory-llnl":
            return implementations.InMemorySQLite3_LLNL()
        case "mariadb-theta":
            return implementations.MariaDB_Theta()
        case "mariadb-llnl":
            return implementations.MariaDB_LLNL()
        case "db2-theta":
            return implementations.DB2_Theta()
        case "db2-llnl":
            return implementations.DB2_LLNL()
        case _:
            return False


# def benchmark_db_theta(
#     iterations: int,
#     db: theta.Theta,
#     directory: Path,
#     benchmark_results_db: implementations.BenchmarkResults_Theta,
# ) -> None:
#     results: DataFrame
#     data: dict[str, list[float]]

#     def _getDatasets() -> Iterator[scoda_dataset.Dataset]:
#         datasets: Iterator[scoda_dataset.Dataset] | bool = (
#             scoda_dataset.load_theta_datasets(
#                 directory=directory,
#             )
#         )

#         if isinstance(datasets, bool):
#             print(
#                 "I'm destroying this process right now because you couldn't point me to a directory containing THETA CSV files."
#             )
#             quit(10)

#         return datasets

#     # Write all tables to a database
#     with Bar("Benchmarking writing all tables to database", max=iterations) as bar:
#         datasets: Iterator[scoda_dataset.Dataset] = _getDatasets()
#         data = defaultdict(list)
#         for _ in range(iterations):
#             data["seconds"].append(
#                 benchmark_write_all_tables(
#                     db=db,
#                     datasets=datasets,
#                 )
#             )
#             bar.next()

#     results = DataFrame(data=data)
#     results.to_sql(
#         name="benchmark_write_all_tables",
#         con=benchmark_results_db.engine,
#         if_exists="append",
#         index=False,
#     )

#     # Write individual tables to a database
#     with Bar(
#         "Benchmarking writing individual tables to database", max=iterations
#     ) as bar:
#         datasets = _getDatasets()
#         data = defaultdict(list)
#         for _ in range(iterations):
#             dataset: scoda_dataset.Dataset
#             for dataset in datasets:
#                 data[dataset.name].append(
#                     benchmark_per_db_table_write(
#                         db=db,
#                         dataset=dataset,
#                     )
#                 )
#             bar.next()

#     results = DataFrame(data=data)
#     results.to_sql(
#         name="benchmark_per_table_write",
#         con=benchmark_results_db.engine,
#         if_exists="append",
#         index=False,
#     )


def benchmark_db_llnl(
    iterations: int,
    db: llnl_last.LLNL_LAST,
    datasets: list[scoda_dataset.Dataset],
    benchmark_results_db: implementations.BenchmarkResults_LLNL,
) -> None:
    benchmark_write_all_tables(
        test_db=db,
        datasets=datasets,
        iterations=iterations,
        benchmark_db=benchmark_results_db,
    )
    benchmark_write_per_table(
        test_db=db,
        datasets=datasets,
        iterations=iterations,
        benchmark_db=benchmark_results_db,
    )
    benchmark_sequential_writes_all_tables(
        test_db=db,
        datasets=datasets,
        iterations=iterations,
        benchmark_db=benchmark_results_db,
    )

    benchmark_sequential_writes_per_tables(
        test_db=db,
        datasets=datasets,
        iterations=iterations,
        benchmark_db=benchmark_results_db,
    )


def main() -> int:
    cli: CLI = CLI()
    args = cli.parse_args().__dict__
    dataset_type: Literal["llnl", "theta"] = "llnl"

    # 0: Connect to database
    db: llnl_last.DB | bool = create_db(db_name=args["db"][0])
    if isinstance(db, bool):
        return 1
    # Clear tables even if there is nothing in them
    db.recreate_tables()

    benchmark_result_db: (
        implementations.BenchmarkResults_LLNL | implementations.BenchmarkResults_Theta
    )

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

from scoda.cli import CLI
import scoda.api.db as scoda_db
import scoda.api.dataset as scoda_dataset
from pathlib import Path


def create_db(db_name: str) -> scoda_db.DB | bool:
    match db_name:
        case "postgres":
            return scoda_db.PostgreSQL()
        case "mysql":
            return scoda_db.MySQL()
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
    datasets: list[scoda_dataset.Dataset],
    db: scoda_db.DB,
    benchmark_result_db: scoda_db.BenchmarkResults,
) -> bool:
    return True


def main() -> int:
    cli: CLI = CLI()
    args = cli.parse_args().__dict__

    # 0: Connect to database
    db: scoda_db.DB | bool = create_db(db_name=args["db"][0])
    if isinstance(db, bool):
        return 1

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

    return 0


if __name__ == "__main__":
    main()

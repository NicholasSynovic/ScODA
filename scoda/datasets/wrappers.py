"""
LAST datasets.

Copyright 2025 (C) Nicholas M. Synovic

"""

import sys
from collections.abc import Iterator
from json import dumps
from pathlib import Path
from typing import Literal

import pandas as pd
from pandas import DataFrame


class Dataset:
    def __init__(
        self,
        name: str,
        fp: Path,
        time_column: str,
        query_column: str,
    ) -> None:
        self.name: str = name  # Name of the dataset
        self.fp: Path = fp  # Path to the dataset
        self.time_column: str = time_column  # Name of the timestamp column
        self.query_column: str = (
            query_column  # Name of the column to query in benchmarks
        )

        self.data: DataFrame = self.read()  # Unformatted data
        self.time_series_data: DataFrame = (
            self._create_time_series_data()
        )  # time series formatted data
        self.json_data: list[dict] = self._create_json_data()  # JSON data
        self.json_data_str: str = dumps(obj=self.json_data)  # Stringified JSON
        self.json_data_list: list[str] = [
            dumps(x) for x in self.json_data
        ]  # List of stringified JSON objects

    def _create_time_series_data(self) -> DataFrame:
        time_series_data = self.data.copy()
        time_series_data[self.time_column] = time_series_data[self.time_column].apply(
            pd.Timestamp
        )

        return time_series_data.set_index(
            keys=self.time_column,
        )

    def _create_json_data(self) -> list[dict]:
        json_data: DataFrame = self.data.copy()
        json_data["name"] = self.name
        return json_data.to_dict(orient="records")

    def read(self) -> DataFrame:
        try:
            data: DataFrame = pd.read_csv(filepath_or_buffer=self.fp)
        except pd.errors.ParserError as pe:
            print(self.name, self.fp, pe)  # noqa: T201
            sys.exit(1)

        data.columns = data.columns.str.replace(pat=" ", repl="_")
        return data


class CoriPower(Dataset):
    """
    Dataset class for Cori Power data.

    Initializes the dataset with the specific file path for Cori Power data.

    """

    def __init__(self, directory: Path) -> None:  # noqa: D107
        fp: Path = Path(directory, "Cori_power_30_sec.csv").resolve()
        super().__init__(
            name="cori_power_30_sec",
            fp=fp,
            time_column="timestamp_secs",
            query_column="measured_kW",
        )


class HawkPower(Dataset):
    """
    Dataset class for Hawk Power data.

    Initializes the dataset with the specific file path for Hawk Power data.

    """

    def __init__(self, directory: Path) -> None:  # noqa: D107
        fp: Path = Path(directory, "Hawk_power_15_min.csv").resolve()
        super().__init__(
            name="hawk_power_15_min",
            fp=fp,
            time_column="timestamp_secs",
            query_column="measured_kW",
        )


class HPCGDPC(Dataset):
    """
    Dataset class for HPCG DPC data.

    Initializes the dataset with the specific file path for HPCG DPC data.

    """

    def __init__(self, directory: Path) -> None:  # noqa: D107
        fp: Path = Path(directory, "hlrs_hpl_hpcg_data/hpcg_dpc.csv").resolve()
        super().__init__(
            name="hpcg_dpc",
            fp=fp,
            time_column="Time",
            query_column="Node r9c1t1n1",
        )


class HPCGSPC(Dataset):
    """
    Dataset class for HPCG SPC data.

    Initializes the dataset with the specific file path for HPCG SPC data.

    """

    def __init__(self, directory: Path) -> None:  # noqa: D107
        fp: Path = Path(directory, "hlrs_hpl_hpcg_data/hpcg_spc.csv").resolve()
        super().__init__(
            name="hpcg_spc",
            fp=fp,
            time_column="Time",
            query_column="Node r6c3t1n1",
        )


class HPCGUC(Dataset):
    """
    Dataset class for HPCG UC data.

    Initializes the dataset with the specific file path for HPCG UC data.

    """

    def __init__(self, directory: Path) -> None:  # noqa: D107
        fp: Path = Path(directory, "hlrs_hpl_hpcg_data/hpcg_uc.csv").resolve()
        super().__init__(
            name="hpcg_uc",
            fp=fp,
            time_column="Time",
            query_column="Node r7c3t1n1",
        )


class HPLDPC(Dataset):
    """
    Dataset class for HPL DPC data.

    Initializes the dataset with the specific file path for HPL DPC data.

    """

    def __init__(self, directory: Path) -> None:  # noqa: D107
        fp: Path = Path(directory, "hlrs_hpl_hpcg_data/hpl_dpc.csv").resolve()
        super().__init__(
            name="hpl_dpc",
            fp=fp,
            time_column="Time",
            query_column="Node r10c1t1n1",
        )


class HPLSPC(Dataset):
    """
    Dataset class for HPL SPC data.

    Initializes the dataset with the specific file path for HPL SPC data.

    """

    def __init__(self, directory: Path) -> None:  # noqa: D107
        fp: Path = Path(directory, "hlrs_hpl_hpcg_data/hpl_spc.csv").resolve()
        super().__init__(
            name="hpl_spc",
            fp=fp,
            time_column="Time",
            query_column="Node r14c3t1n1",
        )


class HPLUC(Dataset):
    """
    Dataset class for HPL UC data.

    Initializes the dataset with the specific file path for HPL UC data.

    """

    def __init__(self, directory: Path) -> None:  # noqa: D107
        fp: Path = Path(directory, "hlrs_hpl_hpcg_data/hpl_uc.csv").resolve()
        super().__init__(
            name="hpl_uc",
            fp=fp,
            time_column="Time",
            query_column="Node r14c3t1n1",
        )


class LumiHPCG(Dataset):
    """
    Dataset class for Lumi HPCG data.

    Initializes the dataset with the specific file path for Lumi HPCG data.

    """

    def __init__(self, directory: Path) -> None:  # noqa: D107
        fp: Path = Path(directory, "lumi_hpcg_data/lumi_hpcg.csv").resolve()
        super().__init__(
            name="lumi_hpcg",
            fp=fp,
            time_column="timestamp_secs",
            query_column="measured_kW",
        )


class LumiPower(Dataset):
    """
    Dataset class for Lumi Power data.

    Initializes the dataset with the specific file path for Lumi Power data.

    """

    def __init__(self, directory: Path) -> None:  # noqa: D107
        fp: Path = Path(directory, "Lumi_power_10_min.csv").resolve()
        super().__init__(
            name="lumi_power_10_min",
            fp=fp,
            time_column="timestamp_secs",
            query_column="measured_kW",
        )


class Marconi100Power(Dataset):
    """
    Dataset class for Marconi100 Power data.

    Initializes the dataset with the specific file path for Marconi100 Power data.

    """

    def __init__(self, directory: Path) -> None:  # noqa: D107
        fp: Path = Path(directory, "Marconi100_power_60_sec.csv").resolve()
        super().__init__(
            name="marconi100_power_60_sec",
            fp=fp,
            time_column="timestamp_secs",
            query_column="measured_kW",
        )


class PerlmutterPower(Dataset):
    """
    Dataset class for Perlmutter Power data.

    Initializes the dataset with the specific file path for Perlmutter Power data.

    """

    def __init__(self, directory: Path) -> None:  # noqa: D107
        fp: Path = Path(directory, "Perlmutter_power_60_sec.csv").resolve()
        super().__init__(
            name="perlmutter_power_60_sec",
            fp=fp,
            time_column="timestamp_secs",
            query_column="measured_kW",
        )


def load_llnl_datasets(
    directory: Path,
) -> list[Dataset] | Literal[False]:
    data: list[Dataset] = []

    try:
        data.extend(
            [
                CoriPower(directory=directory),
                HawkPower(directory=directory),
                HPCGDPC(directory=directory),
                HPCGSPC(directory=directory),
                HPCGUC(directory=directory),
                HPLDPC(directory=directory),
                HPLSPC(directory=directory),
                HPLUC(directory=directory),
                LumiHPCG(directory=directory),
                LumiPower(directory=directory),
                Marconi100Power(directory=directory),
                PerlmutterPower(directory=directory),
            ]
        )
    except FileNotFoundError:
        return False

    return data


def load_theta_datasets(directory: Path) -> Iterator[Dataset]:
    fps: list[Path] = [Path(directory, fp) for fp in Path(directory).iterdir()]

    for fp in fps:
        if not fp.is_file():
            error_message: str = f"Path {fp} is not a file."
            raise FileNotFoundError(error_message)

    for fp in fps:
        yield Dataset(name=fp.stem, fp=fp)

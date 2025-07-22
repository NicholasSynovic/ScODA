"""
LAST datasets.

Copyright 2025 (C) Nicholas M. Synovic

"""

from pathlib import Path
from typing import Literal

import scoda.datasets as scoda_datasets


class CoriPower(scoda_datasets.Dataset):
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


class HawkPower(scoda_datasets.Dataset):
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


class HPCGDPC(scoda_datasets.Dataset):
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


class HPCGSPC(scoda_datasets.Dataset):
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


class HPCGUC(scoda_datasets.Dataset):
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


class HPLDPC(scoda_datasets.Dataset):
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


class HPLSPC(scoda_datasets.Dataset):
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


class HPLUC(scoda_datasets.Dataset):
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


class LumiHPCG(scoda_datasets.Dataset):
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


class LumiPower(scoda_datasets.Dataset):
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


class Marconi100Power(scoda_datasets.Dataset):
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


class PerlmutterPower(scoda_datasets.Dataset):
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
) -> list[scoda_datasets.Dataset] | Literal[False]:
    """
    Load LLNL datasets from the specified directory.

    Args:
        directory (Path): The directory containing the dataset files.

    Returns:
        list[Dataset] | Literal[False]: A list of Dataset instances if
            successful, False if any file is not found.

    """
    data: list[scoda_datasets.Dataset] = []

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

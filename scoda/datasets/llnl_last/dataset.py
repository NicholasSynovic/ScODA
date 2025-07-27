from collections.abc import Iterable
from pathlib import Path

import pandas as pd

import scoda.datasets.generic


def simple_vm_json(
    query_column: str, name: str, time_column: str, df: pd.DataFrame
) -> list[dict]:
    data: list[dict] = []
    tags: dict = {"name": name}

    row: pd.Series
    for _, row in df.iterrows():
        data.append(
            {
                "metric": {
                    "tags": tags,
                },
                "timestamps": [int(row[time_column].timestamp())],
                "values": [row[query_column]],
            }
        )

    return data


class CoriPower(scoda.datasets.generic.Dataset):
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

    def _create_victoriametric_json(self) -> list[dict]:
        return simple_vm_json(
            query_column=self.query_column,
            name=self.name,
            time_column=self.time_column,
            df=self.data,
        )


class HawkPower(scoda.datasets.generic.Dataset):
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

    def _create_victoriametric_json(self) -> list[dict]:
        return simple_vm_json(
            query_column=self.query_column,
            name=self.name,
            time_column=self.time_column,
            df=self.data,
        )


class HPCGDPC(scoda.datasets.generic.Dataset):
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
            query_column="Node_r9c1t1n1",
        )

    def _create_victoriametric_json(self) -> list[dict]:
        return simple_vm_json(
            query_column=self.query_column,
            name=self.name,
            time_column=self.time_column,
            df=self.data,
        )


class HPCGSPC(scoda.datasets.generic.Dataset):
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
            query_column="Node_r6c3t1n1",
        )

    def _create_victoriametric_json(self) -> list[dict]:
        return simple_vm_json(
            query_column=self.query_column,
            name=self.name,
            time_column=self.time_column,
            df=self.data,
        )


class HPCGUC(scoda.datasets.generic.Dataset):
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
            query_column="Node_r7c3t1n1",
        )

    def _create_victoriametric_json(self) -> list[dict]:
        return simple_vm_json(
            query_column=self.query_column,
            name=self.name,
            time_column=self.time_column,
            df=self.data,
        )


class HPLDPC(scoda.datasets.generic.Dataset):
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
            query_column="Node_r10c1t1n1",
        )

    def _create_victoriametric_json(self) -> list[dict]:
        return simple_vm_json(
            query_column=self.query_column,
            name=self.name,
            time_column=self.time_column,
            df=self.data,
        )


class HPLSPC(scoda.datasets.generic.Dataset):
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
            query_column="Node_r14c3t1n1",
        )

    def _create_victoriametric_json(self) -> list[dict]:
        return simple_vm_json(
            query_column=self.query_column,
            name=self.name,
            time_column=self.time_column,
            df=self.data,
        )


class HPLUC(scoda.datasets.generic.Dataset):
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
            query_column="Node_r14c3t1n1",
        )

    def _create_victoriametric_json(self) -> list[dict]:
        return simple_vm_json(
            query_column=self.query_column,
            name=self.name,
            time_column=self.time_column,
            df=self.data,
        )


class LumiHPCG(scoda.datasets.generic.Dataset):
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

    def _create_victoriametric_json(self) -> list[dict]:
        return simple_vm_json(
            query_column=self.query_column,
            name=self.name,
            time_column=self.time_column,
            df=self.data,
        )


class LumiPower(scoda.datasets.generic.Dataset):
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

    def _create_victoriametric_json(self) -> list[dict]:
        return simple_vm_json(
            query_column=self.query_column,
            name=self.name,
            time_column=self.time_column,
            df=self.data,
        )


class Marconi100Power(scoda.datasets.generic.Dataset):
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

    def _create_victoriametric_json(self) -> list[dict]:
        return simple_vm_json(
            query_column=self.query_column,
            name=self.name,
            time_column=self.time_column,
            df=self.data,
        )


class PerlmutterPower(scoda.datasets.generic.Dataset):
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

    def _create_victoriametric_json(self) -> list[dict]:
        return simple_vm_json(
            query_column=self.query_column,
            name=self.name,
            time_column=self.time_column,
            df=self.data,
        )


def load_llnl_last(directory: Path) -> Iterable[scoda.datasets.generic.Dataset]:
    return [
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

from pathlib import Path
from abc import ABC
from pandas import DataFrame
import pandas as pd


class Dataset(ABC):
    def __init__(self, name: str, fp: Path) -> None:
        self.name: str = name
        self.fp: Path = fp
        self.data: DataFrame = self.read()

    def read(self) -> DataFrame:
        return pd.read_csv(filepath_or_buffer=self.fp)


class CoriPower(Dataset):
    def __init__(self, directory: Path) -> None:
        fp: Path = Path(directory, "Cori_power_30_sec.csv").resolve()
        super().__init__(name="cori_power_30_sec", fp=fp)


class HawkPower(Dataset):
    def __init__(self, directory: Path) -> None:
        fp: Path = Path(directory, "Hawk_power_15_min.csv").resolve()
        super().__init__(name="hawk_power_15_min", fp=fp)


class HPCGDPC(Dataset):
    def __init__(self, directory: Path) -> None:
        fp: Path = Path(directory, "hlrs_hpl_hpcg_data/hpcg_dpc.csv").resolve()
        super().__init__(name="hpcg_dpc", fp=fp)


class HPCGSPC(Dataset):
    def __init__(self, directory: Path) -> None:
        fp: Path = Path(directory, "hlrs_hpl_hpcg_data/hpcg_spc.csv").resolve()
        super().__init__(name="hpcg_spc", fp=fp)


class HPCGUC(Dataset):
    def __init__(self, directory: Path) -> None:
        fp: Path = Path(directory, "hlrs_hpl_hpcg_data/hpcg_uc.csv").resolve()
        super().__init__(name="hpcg_uc", fp=fp)


class HPLDPC(Dataset):
    def __init__(self, directory: Path) -> None:
        fp: Path = Path(directory, "hlrs_hpl_hpcg_data/hpl_dpc.csv").resolve()
        super().__init__(name="hpl_dpc", fp=fp)


class HPLSPC(Dataset):
    def __init__(self, directory: Path) -> None:
        fp: Path = Path(directory, "hlrs_hpl_hpcg_data/hpl_spc.csv").resolve()
        super().__init__(name="hpl_spc", fp=fp)


class HPLUC(Dataset):
    def __init__(self, directory: Path) -> None:
        fp: Path = Path(directory, "hlrs_hpl_hpcg_data/hpl_uc.csv").resolve()
        super().__init__(name="hpl_uc", fp=fp)


class LumiHPCG(Dataset):
    def __init__(self, directory: Path) -> None:
        fp: Path = Path(directory, "lumi_hpcg_data/lumi_hpcg.csv").resolve()
        super().__init__(name="lumi_hpcg", fp=fp)


class LumiPower(Dataset):
    def __init__(self, directory: Path) -> None:
        fp: Path = Path(directory, "Lumi_power_10_min.csv").resolve()
        super().__init__(name="lumi_power_10_min", fp=fp)


class Marconi100Power(Dataset):
    def __init__(self, directory: Path) -> None:
        fp: Path = Path(directory, "Marconi100_power_60_sec.csv").resolve()
        super().__init__(name="marconi100_power_60_sec", fp=fp)


class PerlmutterPower(Dataset):
    def __init__(self, directory: Path) -> None:
        fp: Path = Path(directory, "Perlmutter_power_60_sec.csv").resolve()
        super().__init__(name="perlmutter_power_60_sec", fp=fp)

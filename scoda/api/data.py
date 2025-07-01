import pandas as pd
from pandas import DataFrame
from pathlib import Path
from typing import Literal

DATA_DIR: Path = Path("../data/LAST/Power-Provisioning-Dataset").resolve()


def read_csv(dataset: Literal["cori_30s"]) -> DataFrame:
    fp: Path = Path()

    match dataset:
        case "cori_30s":
            fp = Path(DATA_DIR, "Cori_power_30_sec.csv")

    return pd.read_csv(filepath_or_buffer=fp)

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


def load_theta_datasets(directory: Path) -> Iterator[Dataset]:
    fps: list[Path] = [Path(directory, fp) for fp in Path(directory).iterdir()]

    for fp in fps:
        if not fp.is_file():
            error_message: str = f"Path {fp} is not a file."
            raise FileNotFoundError(error_message)

    for fp in fps:
        yield Dataset(name=fp.stem, fp=fp)

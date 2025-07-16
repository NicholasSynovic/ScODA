"""
THETA dataset classes.

Copyright 2025 (C) Nicholas M. Synovic

"""

from pathlib import Path
from collections.abc import Iterator
import scoda.datasets as scoda_datasets
import os


def load_theta_datasets(directory: Path) -> Iterator[scoda_datasets.Dataset]:
    fps: list[Path] = [Path(directory, fp) for fp in os.listdir(path=directory)]

    fp: Path
    for fp in fps:
        if fp.is_file() == False:
            return False
    for fp in fps:
        yield scoda_datasets.Dataset(name=fp.stem, fp=fp)

"""
THETA dataset classes.

Copyright 2025 (C) Nicholas M. Synovic

"""

from pathlib import Path
from collections.abc import Iterator
import scoda.datasets as scoda_datasets


def load_theta_datasets(directory: Path) -> Iterator[scoda_datasets.Dataset]:
    """
    Load Theta datasets from the specified directory.

    This function iterates over all files in the specified directory, creating a
    Dataset instance for each file. If any path in the directory is not a file,
    the function raises a FileNotFoundError.

    Args:
        directory (Path): The directory containing the dataset files.

    Yields:
        scoda_datasets.Dataset: An instance of Dataset for each file in the
            directory.

    Raises:
        FileNotFoundError: If any path in the directory is not a file.

    """
    fps: list[Path] = [Path(directory, fp) for fp in Path(directory).iterdir()]

    for fp in fps:
        if not fp.is_file():
            error_message: str = f"Path {fp} is not a file."
            raise FileNotFoundError(error_message)

    for fp in fps:
        yield scoda_datasets.Dataset(name=fp.stem, fp=fp)

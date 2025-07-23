from collections.abc import Iterator
from pathlib import Path

import scoda.datasets.generic


class Theta(scoda.datasets.generic.Dataset):
    def __init__(self, fp: Path, name: str = "anl_theta_0") -> None:
        super().__init__(
            name=name,
            fp=fp.resolve(),
            time_column="time_secs",
            query_column="",
        )


def load_anl_theta(directory: Path) -> Iterator[scoda.datasets.generic.Dataset]:
    fps: list[Path] = [
        Path(directory, fp).resolve() for fp in Path(directory).iterdir()
    ]

    for fp in fps:
        if not fp.is_file():
            error_message: str = f"Path {fp} is not a file."
            raise FileNotFoundError(error_message)

    count: int = 0
    for fp in fps:
        name: str = f"anl_theta_{count}"
        count += 1
        yield Theta(fp=fp, name=name)

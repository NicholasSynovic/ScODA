"""
Dataset base class.

Copyright 2025 (C) Nicholas M. Synovic

"""

import sys
from json import dumps
from pathlib import Path
from typing import Any

import pandas as pd
from pandas import DataFrame


class Dataset:
    """
    Base class for datasets.

    This class provides a common interface for reading CSV files into a
    DataFrame, converting the data to JSON-compatible formats, and storing
    metadata.

    Attributes:
        name (str): The name of the dataset.
        fp (Path): The file path to the dataset CSV file.
        data (DataFrame): The data read from the CSV file.
        json_dict (list[Any]): The data converted to a list of dictionaries for
            JSON serialization.
        json_str (str): The data serialized as a JSON string.
        json_list_str (list[str]): The data serialized as a list of JSON strings.

    """

    def __init__(self, name: str, fp: Path) -> None:
        """
        Initialize the Dataset with a name and file path.

        Args:
            name (str): The name of the dataset.
            fp (Path): The file path to the dataset CSV file.

        """
        self.name: str = name
        self.fp: Path = fp
        self.data: DataFrame = self.read()

        json_compatible_data: DataFrame = self.data.copy()
        json_compatible_data["name"] = self.name

        self.json_dict: list[Any] = json_compatible_data.to_dict(
            orient="records",
        )

        self.json_str: str = dumps(self.json_dict)
        self.json_list_str: list[str] = [dumps(x) for x in self.json_dict]

    def read(self) -> DataFrame:
        """
        Read the dataset from a CSV file into a DataFrame.

        Returns:
            DataFrame: The data read from the CSV file.

        """
        try:
            data: DataFrame = pd.read_csv(filepath_or_buffer=self.fp)
        except pd.errors.ParserError as pe:
            print(self.name, self.fp, pe)  # noqa: T201
            sys.exit(1)

        data.columns = data.columns.str.replace(pat=" ", repl="_")
        return data

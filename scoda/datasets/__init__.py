"""
Dataset base class.

Copyright 2025 (C) Nicholas M. Synovic

"""

import sys
from json import dumps
from pathlib import Path

import pandas as pd
from pandas import DataFrame


class Dataset:
    def __init__(self, name: str, fp: Path, time_column: str) -> None:
        self.name: str = name  # Name of the dataset
        self.fp: Path = fp  # Path to the dataset
        self.time_column: str = time_column  # Name of the timestamp column

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

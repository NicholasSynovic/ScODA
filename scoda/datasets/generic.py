"""
Generic Dataset.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import sys
from json import dumps
from pathlib import Path

import pandas as pd
from pandas import DataFrame


def simple_vm_json(
    query_column: str,
    name: str,
    time_column: str,
    df: pd.DataFrame,
) -> list[dict]:
    """
    Convert a DataFrame into a list[dict] suitable for Victoria Metrics.

    This function iterates through each row of the input DataFrame and formats
    the specified columns into a dictionary structure, including metric tags,
    timestamps, and values.

    Arguments:
        query_column: The name of the DataFrame column to use as the metric
            value.
        name: The name to assign to the metric.
        time_column: The name of the DataFrame column containing timestamp
            information.
        df: The Pandas DataFrame to process.

    Returns:
        A list of dictionaries, each representing a formatted metric entry.

    """
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


class Dataset:
    """
    Represent a generic dataset with data loading and formatting capabilities.

    This class provides core functionalities for reading data from a file,
    handling time series conversions, and formatting data into JSON and
    VictoriaMetrics-compatible structures.

    Arguments:
        name: The name of the dataset.
        fp: The file path to the dataset.
        time_column: The name of the column containing timestamp information.
        query_column: The name of the column to be used for queries or
            benchmarks.

    """

    def __init__(
        self,
        name: str,
        fp: Path,
        time_column: str,
        query_column: str,
    ) -> None:
        """
        Initialize the Dataset instance.

        This constructor sets up the dataset's name, file path, and column names
        for time and query data. It also reads the data, fills missing values,
        and generates various formatted versions of the data, including time
        series, JSON, and VictoriaMetrics-compatible JSON.

        Arguments:
            name: The name of the dataset.
            fp: The path to the dataset file.
            time_column: The name of the timestamp column in the dataset.
            query_column: The name of the column to be used for querying or
                benchmarking.

        """
        self.name: str = name  # Name of the dataset
        self.fp: Path = fp  # Path to the dataset
        self.time_column: str = time_column  # Name of the timestamp column
        self.query_column: str = (
            query_column  # Name of the column to query in benchmarks
        )

        self.data: DataFrame = self.read()  # Unformatted data
        self.data = self.data.fillna(value=-1)
        self.data_size: int = self.data.shape[0]

        self.time_series_data: DataFrame = (
            self._create_time_series_data()
        )  # time series formatted data
        self.json_data: list[dict] = self._create_json_data()  # JSON data
        self.json_data_str: str = dumps(obj=self.json_data)  # Stringified JSON
        self.json_data_list: list[str] = [
            dumps(x) for x in self.json_data
        ]  # List of stringified JSON objects

        self.victoriametric_json: list[dict] = self._create_victoriametric_json()

    def _create_time_series_data(self) -> DataFrame:
        """
        Create a time series formatted DataFrame from the raw data.

        This method copies the raw data, converts the time column to pandas
        Timestamps, and sets it as the DataFrame index, resulting in a time
        series DataFrame.

        Arguments:
            None

        Returns:
            A Pandas DataFrame formatted as a time series.

        """
        time_series_data = self.data.copy()
        time_series_data[self.time_column] = time_series_data[self.time_column].apply(
            pd.Timestamp
        )

        return time_series_data.set_index(
            keys=self.time_column,
        )

    def _create_json_data(self) -> list[dict]:
        """
        Create a list of dictionaries (JSON-compatible) from the raw data.

        This method copies the raw data, adds a 'name' column, converts the
        time column to string format, adds an 'id' column based on the index,
        and then converts the DataFrame into a list of dictionaries.

        Arguments:
            None

        Returns:
            A list of dictionaries, each representing a row of the DataFrame
            in a JSON-compatible format.

        """
        json_data: DataFrame = self.data.copy()
        json_data["name"] = self.name
        json_data[self.time_column] = json_data[self.time_column].apply(str)
        json_data["id"] = json_data.index
        return json_data.to_dict(orient="records")

    def read(self) -> DataFrame:
        """
        Read the dataset from the specified file path into a Pandas DataFrame.

        This method attempts to read a CSV file from the `fp` attribute.
        It handles potential parsing errors, replaces spaces in column names
        with underscores, and converts the time column to UTC-aware datetime objects.

        Arguments:
            None

        Returns:
            A Pandas DataFrame containing the raw data from the file.

        """
        try:
            data: DataFrame = pd.read_csv(filepath_or_buffer=self.fp)
        except pd.errors.ParserError as pe:
            print(self.name, self.fp, pe)  # noqa: T201
            sys.exit(1)

        data.columns = data.columns.str.replace(pat=" ", repl="_")
        data[self.time_column] = pd.to_datetime(
            data[self.time_column],
            utc=True,
        )
        return data

    def _create_victoriametric_json(self) -> list[dict]:
        """
        Generate VictoriaMetrics-compatible JSON from the dataset.

        This method utilizes the `simple_vm_json` helper function to convert
        the dataset's internal data (DataFrame) into a list of dictionaries
        formatted for ingestion by VictoriaMetrics.

        Arguments:
            None

        Returns:
            A list of dictionaries, each formatted for VictoriaMetrics ingestion.

        """
        return simple_vm_json(
            query_column=self.query_column,
            name=self.name,
            time_column=self.time_column,
            df=self.data,
        )

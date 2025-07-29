from abc import ABC, abstractmethod

import scoda.datasets.generic


class DB(ABC):
    def __init__(self, convert_time_column_to_int: bool = False) -> None:
        self.convert_time_column_to_int: bool = convert_time_column_to_int

    @abstractmethod
    def batch_upload(self, dataset: scoda.datasets.generic.Dataset) -> None:
        """
        Upload data in batch to the database.

        This method iterates over a dataset and sends each record to the
        database.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` instance.

        """
        ...

    @abstractmethod
    def batch_read(self, table_name: str) -> None:
        """
        Perform a batch read of all metrics from the database.

        Arguments:
            table_name: The name of the table to read from.

        """
        ...

    @abstractmethod
    def create(self) -> None: ...

    @abstractmethod
    def delete(self) -> None:
        """
        Delete all data from the database.

        This method sends removes all stored data.

        """
        ...

    @abstractmethod
    def query_average_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query the average value of a column in a given table.

        This method retrieves the average value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the average value.

        """
        ...

    @abstractmethod
    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query a time window value grouped by a table name and column name.

        This function sends a request to group a specified column over an one
        hour time window.

        Arguments:
            table_name: The name of the table to group by.
            column_name: The name of the column to average over time.

        """
        ...

    @abstractmethod
    def query_max_value(self, table_name: str, column_name: str) -> None:
        """
        Query the maximum value of a column in a given table.

        This method retrieves the maximum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the maximum value.

        """
        ...

    @abstractmethod
    def query_min_value(self, table_name: str, column_name: str) -> None:
        """
        Query the minimum value of a column in a given table.

        This method retrieves the minimum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the minimum value.

        """
        ...

    @abstractmethod
    def query_mode_value(self, table_name: str, column_name: str) -> None:
        """
        Query the mode value from a specified table and column.

        It is intended to support analysis for determining the most frequent
        (mode) value within the specified table and column.

        Arguments:
            table_name: The name of the table to query.
            column_name: The name of the column to extract the mode value from.

        """
        ...

    def recreate(self) -> None:
        self.delete()
        self.create()

    @abstractmethod
    def sequential_read(self, table_name: str, rows: int) -> None:
        """
        Perform a sequential read of records from a specified table.

        This method constructs and executes a  query to read all records from
        the specified table. It counts the number of records streamed from the
        query result but does not return or store them.

        Arguments:
            table_name: The name of the table (i.e., measurement) to read from.
            rows: The number of rows to read.

        """
        ...

    @abstractmethod
    def sequential_upload(
        self,
        dataset: scoda.datasets.generic.Dataset,
    ) -> None:
        """
        Upload data sequentially to the database.

        This method iterates over each row in a dataset and writes it
        individually to the database with a short delay between uploads. Each
        row is converted to a line protocol point with appropriate tags and
        fields. Invalid or missing field values are skipped, and non-numeric
        strings are cast to float where possible.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` containing time series data
                    with a time index and one or more value columns.

        """
        ...

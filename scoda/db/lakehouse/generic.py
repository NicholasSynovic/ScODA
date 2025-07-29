from abc import abstractmethod

import scoda.datasets.generic
import scoda.db


class LakehouseDB(scoda.db.DB):
    def __init__(
        self,
        convert_time_column_to_int: bool = True,
    ) -> None:
        super().__init__(convert_time_column_to_int=convert_time_column_to_int)

    @abstractmethod
    def batch_upload(self, dataset: scoda.datasets.generic.Dataset) -> None: ...

    @abstractmethod
    def batch_read(self, table_name: str) -> None: ...

    @abstractmethod
    def create(self) -> None: ...

    @abstractmethod
    def delete(self) -> None: ...

    @abstractmethod
    def query_average_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

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
    def query_max_value(self, table_name: str, column_name: str) -> None: ...

    @abstractmethod
    def query_min_value(self, table_name: str, column_name: str) -> None: ...

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

    @abstractmethod
    def sequential_read(self, table_name: str, rows: int) -> None: ...

    @abstractmethod
    def sequential_upload(self, dataset: scoda.datasets.generic.Dataset) -> None: ...

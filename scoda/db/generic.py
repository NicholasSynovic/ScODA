from abc import ABC, abstractmethod

import scoda.datasets.generic


class DB(ABC):
    def __init__(self, convert_time_column_to_int: bool = False) -> None:
        self.convert_time_column_to_int: bool = convert_time_column_to_int

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
    ) -> None: ...

    @abstractmethod
    def query_max_value(self, table_name: str, column_name: str) -> None: ...

    @abstractmethod
    def query_min_value(self, table_name: str, column_name: str) -> None: ...

    @abstractmethod
    def query_mode_value(self, table_name: str, column_name: str) -> None: ...

    def recreate(self) -> None:
        self.delete()
        self.create()

    @abstractmethod
    def sequential_read(self, table_name: str, rows: int) -> None: ...

    @abstractmethod
    def sequential_upload(self, dataset: scoda.datasets.generic.Dataset) -> None: ...

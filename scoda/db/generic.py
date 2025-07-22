from abc import ABC, abstractmethod

import scoda.datasets


class DB(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def batch_upload(self, data: scoda.datasets.Dataset) -> None: ...

    @abstractmethod
    def batch_read(self, table_name: str) -> None: ...

    @abstractmethod
    def create(self) -> None: ...

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

    @abstractmethod
    def recreate(self) -> None: ...

    @abstractmethod
    def sequential_read(self, table_name: str, rows: int) -> None: ...

    @abstractmethod
    def sequential_upload(self, data: scoda.datasets.Dataset) -> None: ...

from abc import ABC
from sqlalchemy import Engine, create_engine, MetaData, select, func, Table
from abc import abstractmethod
from typing import Any
from requests import Response


class DB(ABC):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str = "research",
    ):
        self.uri: str = uri
        self.username: str = username
        self.password: str = password
        self.database: str = database

    @abstractmethod
    def create(self) -> None: ...

    @abstractmethod
    def recreate(self) -> None: ...

    @abstractmethod
    def batch_upload(self, data: Any) -> None: ...

    @abstractmethod
    def sequential_upload(self, data: Any) -> None: ...


class DocumentDB(ABC):
    def __init__(self, url: str, username: str, password: str) -> None:
        self.url = url
        self.username: str = username
        self.password: str = password
        self.database_name: str = "research"

    @abstractmethod
    def create_database(self) -> None: ...

    @abstractmethod
    def upload(self, data: str) -> Response: ...

    @abstractmethod
    def query_avg_value(self) -> None: ...

    @abstractmethod
    def query_min_value(self) -> None: ...

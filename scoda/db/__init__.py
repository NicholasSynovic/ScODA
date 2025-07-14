from abc import ABC
from abc import abstractmethod
from typing import Any


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

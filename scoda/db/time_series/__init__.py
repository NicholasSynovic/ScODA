from abc import abstractmethod

import scoda.api.dataset as scoda_dataset
import scoda.db as scoda_db


class TimeSeriesDB(scoda_db.DB):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str = "research",
    ):
        super().__init__(uri, username, password, database)

        self.create()

    @abstractmethod
    def create(self) -> None: ...

    @abstractmethod
    def recreate(self) -> None: ...

    @abstractmethod
    def batch_upload(self, data: scoda_dataset.Dataset) -> None: ...

    @abstractmethod
    def sequential_upload(self, data: scoda_dataset.Dataset) -> None: ...

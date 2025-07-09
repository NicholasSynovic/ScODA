from abc import ABC
from sqlalchemy import Engine, create_engine, MetaData
from abc import abstractmethod


class DB(ABC):
    def __init__(self, uri: str) -> None:
        self.uri: str = uri
        self.engine: Engine = create_engine(url=self.uri)
        self.metadata: MetaData = MetaData()

        self.create_tables()

    def recreate_tables(self) -> None:
        self.metadata.reflect(bind=self.engine)
        self.metadata.drop_all(bind=self.engine)
        self.metadata.create_all(bind=self.engine, checkfirst=True)

    @abstractmethod
    def create_tables(self) -> None: ...

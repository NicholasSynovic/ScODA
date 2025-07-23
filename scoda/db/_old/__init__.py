"""
Testing database base class.

Copyright 2025 (C) Nicholas M. Synovic
"""

from abc import ABC, abstractmethod

import scoda.datasets.generic
from scoda.db.results import Results


class DB(ABC):
    """
    Abstract base class for testing database interactions.

    This class defines the interface for database operations such as creation,
    recreation, and data uploads. It serves as a blueprint for concrete database
    implementations.

    Attributes:
        uri (str): The URI for connecting to the database.
        username (str): The username for database authentication.
        password (str): The password for database authentication.
        database (str): The name of the database, defaulting to "research".

    """

    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str = "research",
    ) -> None:
        """
        Initialize the DB instance with connection parameters.

        Args:
            uri (str): The URI for connecting to the database.
            username (str): The username for database authentication.
            password (str): The password for database authentication.
            database (str): The name of the database, defaulting to "research".

        """
        self.uri: str = uri
        self.username: str = username
        self.password: str = password
        self.database: str = database

    @abstractmethod
    def batch_upload(self, data: scoda.datasets.generic.Dataset) -> None:
        """
        Upload all documents in one pass.

        This method should be implemented by subclasses to define how data is
        uploaded in a batch to the database.

        Args:
            data (scoda.datasets.generic.Dataset): The data to be uploaded.

        """
        ...

    @abstractmethod
    def batch_read(self, table_name: str) -> None:
        """
        Read all documents in one pass.

        This method should be implemented by subclasses to define how data is
        batch read to the database.

        Args:
            table_name (str): The table to be read.

        """
        ...

    @abstractmethod
    def create(self) -> None:
        """
        Create database tables.

        This method should be implemented by subclasses to define how tables
        are created in the database.

        """
        ...

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
    def query_max_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    @abstractmethod
    def query_min_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    @abstractmethod
    def query_mode_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    @abstractmethod
    def recreate(self) -> None:
        """
        Delete tables and recreate them.

        This method should be implemented by subclasses to define how tables
        are deleted and recreated in the database.

        """
        ...

    @abstractmethod
    def sequential_read(self, table_name: str, rows: int) -> None:
        """
        Read documents one by one.

        This method should be implemented by subclasses to define how data is
        read sequentially from the database.

        Args:
            table_name (str): The table to be read.
            rows (int): The number of rows to be read

        """
        ...

    @abstractmethod
    def sequential_upload(self, data: scoda.datasets.generic.Dataset) -> None:
        """
        Upload documents one by one.

        This method should be implemented by subclasses to define how data is
        uploaded sequentially to the database.

        Args:
            data (scoda.datasets.generic.Dataset): The data to be uploaded.

        """
        ...

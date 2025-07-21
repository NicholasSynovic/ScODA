"""
Document database base class.

Copyright 2025 (C) Nicholas M. Synovic
"""

from abc import abstractmethod

import scoda.datasets as scoda_dataset
import scoda.db as scoda_db


class DocumentDB(scoda_db.DB):
    """
    Base class for document databases.

    Args:
        uri (str): The URI of the database.
        username (str): The username for database authentication.
        password (str): The password for database authentication.
        database (str, optional): The name of the database. Defaults to
            "research".

    """

    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str = "research",
    ) -> None:
        """
        Initialize the DocumentDB instance and create the database.

        Args:
            uri (str): The URI of the database.
            username (str): The username for database authentication.
            password (str): The password for database authentication.
            database (str, optional): The name of the database. Defaults to
                "research".

        """
        super().__init__(
            uri=uri,
            username=username,
            password=password,
            database=database,
        )

        self.create()

    @abstractmethod
    def create(self) -> None:
        """Create the database structure."""
        ...

    @abstractmethod
    def recreate(self) -> None:
        """Recreate the database structure."""
        ...

    @abstractmethod
    def batch_upload(self, data: scoda_dataset.Dataset) -> None:
        """
        Upload data to the database in batch mode.

        Args:
            data (scoda_dataset.Dataset): The dataset to be uploaded.

        """
        ...

    @abstractmethod
    def sequential_upload(self, data: scoda_dataset.Dataset) -> None:
        """
        Upload data to the database sequentially.

        Args:
            data (scoda_dataset.Dataset): The dataset to be uploaded.

        """
        ...

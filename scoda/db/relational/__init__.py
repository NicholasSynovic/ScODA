"""
Relational database base class.

Copyright 2025 (C) Nicholas M. Synovic
"""

from abc import abstractmethod

from sqlalchemy import (
    Engine,
    MetaData,
    Table,
    create_engine,
    func,
    select,
)

import scoda.datasets as scoda_dataset
import scoda.db as scoda_db


class RDBMS(scoda_db.DB):
    """
    Base class for relational database management systems.

    This class provides a foundation for creating and managing a relational database
    using SQLAlchemy. It includes methods for creating the database schema, uploading
    data, and querying specific values from tables.

    Args:
        uri (str): The URI for connecting to the database.
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
        Initialize the RDBMS instance with connection details.

        Args:
            uri (str): The URI for connecting to the database.
            username (str): The username for database authentication.
            password (str): The password for database authentication.
            database (str, optional): The name of the database. Defaults to
                "research".

        """
        super().__init__(uri, username, password, database)
        self.engine: Engine = create_engine(url=self.uri)
        self.metadata: MetaData = MetaData()

        self.create()

    @abstractmethod
    def create(self) -> None:
        """Create the database schema."""
        ...

    def recreate(self) -> None:
        """Recreate the database schema by dropping and creating all tables."""
        self.metadata.reflect(bind=self.engine)
        self.metadata.drop_all(bind=self.engine)
        self.metadata.create_all(bind=self.engine, checkfirst=True)

    def batch_upload(self, data: scoda_dataset.Dataset) -> None:
        """
        Upload data to the database in batch mode.

        Args:
            data (scoda_dataset.Dataset): The dataset to be uploaded.

        """
        data.data.to_sql(
            name=data.name,
            con=self.engine,
            if_exists="append",
            index=False,
        )

    def sequential_upload(self, data: scoda_dataset.Dataset) -> None:
        """
        Upload data to the database sequentially.

        Args:
            data (scoda_dataset.Dataset): The dataset to be uploaded.

        """
        data.data.to_sql(
            name=data.name,
            con=self.engine,
            if_exists="append",
            index=False,
            chunksize=1,
        )

    def query_min_value(self, table_name: str, column_name: str) -> float:
        """
        Query the minimum value from a specified column in a table.

        Args:
            table_name (str): The name of the table to query.
            column_name (str): The name of the column to find the minimum value.

        Returns:
            float: The minimum value found in the specified column.

        """
        table: Table = Table(
            table_name,
            self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            minimum_value_query = select(func.min(table.c[column_name]))
            result = connection.execute(minimum_value_query)
            connection.close()

        return result.scalar()

    def query_avg_value(self, table_name: str, column_name: str) -> float:
        """
        Query the average value from a specified column in a table.

        Args:
            table_name (str): The name of the table to query.
            column_name (str): The name of the column to find the average value.

        Returns:
            float: The average value found in the specified column.

        """
        table: Table = Table(
            table_name,
            self.metadata,
            autoload_with=self.engine,
        )

        with self.engine.connect() as connection:
            average_value_query = select(func.avg(table.c[column_name]))
            result = connection.execute(average_value_query)
            connection.close()

        return result.scalar()

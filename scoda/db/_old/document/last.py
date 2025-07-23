from typing import Any

import pymongo.synchronous.database as pymongo_database
import redis
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
from requests import Response, delete, get, post, put
from requests.auth import HTTPBasicAuth

import scoda.datasets as scoda_dataset
import scoda.db.document as scoda_document


class LAST(scoda_document.DocumentDB):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str = "research",
    ):
        self.db_uri: str = uri + "/" + database
        self.auth: HTTPBasicAuth = HTTPBasicAuth(
            username=username,
            password=password,
        )
        super().__init__(
            uri=uri,
            username=username,
            password=password,
            database=database,
        )


class MongoDB(LAST):
    """
    A class to interact with a MongoDB database for research purposes.

    This class provides methods to create, recreate, and upload data to a MongoDB instance.
    It inherits from the LAST class, which is assumed to provide necessary authentication
    and database URI configuration.

    Attributes:
        client (MongoClient): The MongoDB client used for database operations.
        db (Database): The MongoDB database instance.
    """

    def __init__(self):
        """
        Initializes the MongoDB instance with default connection parameters.

        Sets up the MongoDB client and database using default connection details.
        """
        uri: str = "mongodb://root:example@localhost:27017"
        database_name: str = "research"

        self.collection: str = "research_data"
        self.client: MongoClient = MongoClient(uri)

        self.conn: pymongo_database.Database = self.client[database_name]
        self.conn.au

        super().__init__(
            uri="mongodb://localhost:27017",
            username="root",
            password="example",
            database=database_name,
        )

    def create(self) -> None:
        """
        Creates a collection in the MongoDB database if it does not already exist.

        Checks for the existence of the collection and creates it if necessary.
        """
        try:
            self.conn.create_collection(self.database)
        except CollectionInvalid:
            print("Collection already exists.")

    def recreate(self) -> None:
        """
        Deletes and recreates the collection in the MongoDB database.

        Drops the existing collection and then calls the `create` method to create a new collection.
        """
        self.conn.drop_collection(self.collection)
        self.create()

    def batch_upload(self, data: scoda_dataset.Dataset) -> None:
        """
        Uploads data to the MongoDB database in a single batch operation.

        Inserts the entire dataset into the collection.

        Args:
            data (scoda_dataset.Dataset): The dataset to be uploaded, which should
                                          provide a `json_list` attribute for serialization.
        """
        self.conn[self.collection].insert_many(data.json_dict)

    def sequential_upload(self, data: scoda_dataset.Dataset) -> None:
        """
        Uploads data to the MongoDB database sequentially, one document at a time.

        Iterates over a list of JSON documents, inserting each one individually.

        Args:
            data (scoda_dataset.Dataset): The dataset to be uploaded, which should
                                          provide a `json_list` attribute containing
                                          individual JSON documents.
        """
        document: dict
        for document in data.json_dict:
            self.conn[self.collection].insert_one(document)

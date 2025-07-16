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
        super().__init__(uri, username, password, database)


class CouchDB(LAST):
    """
    A class to interact with a CouchDB database for research purposes.

    This class provides methods to create, recreate, and upload data to a
    CouchDB instance. It inherits from the LAST class, which is assumed to
    provide necessary authentication and database URI configuration.

    Attributes:
        headers (dict[str, str]): HTTP headers used for requests, defaulting to
            JSON content type.
    """

    def __init__(self):
        """
        Initialize the CouchDB instance with default connection parameters.

        Sets up the HTTP headers and calls the superclass initializer with
        default CouchDB connection details including URI, username, password,
        and database name.

        """
        self.headers: dict[str, str] = {"Content-Type": "application/json"}
        super().__init__(
            uri="http://localhost:5984",
            username="root",
            password="example",
            database="research",
        )

    def create(self) -> None:
        """
        Creates the CouchDB database if it does not already exist.

        Sends a GET request to check the existence of the database. If the
        database does not exist (indicated by a non-200 status code), a PUT
        request is sent to create the database.
        """
        resp: Response = get(self.db_uri, auth=self.auth, timeout=600)
        if resp.status_code != 200:
            put(url=self.db_uri, auth=self.auth, timeout=600)

    def recreate(self) -> None:
        """
        Deletes and recreates the CouchDB database.

        Sends a DELETE request to remove the existing database and then calls
        the `create` method to create a new instance of the database.
        """
        delete(url=self.db_uri, auth=self.auth, timeout=600)
        self.create()

    def batch_upload(self, data: scoda_dataset.Dataset) -> None:
        """
        Uploads data to the CouchDB database in a single batch operation.

        Sends a POST request with the entire dataset serialized as a JSON string.

        Args:
            data (scoda_dataset.Dataset): The dataset to be uploaded, which should
                provide a `json_str` attribute for serialization.
        """
        post(
            url=self.db_uri,
            auth=self.auth,
            headers=self.headers,
            data=data.json_str,
            timeout=600,
        )

    def sequential_upload(self, data: scoda_dataset.Dataset) -> None:
        """
        Uploads data to the CouchDB database sequentially, one JSON object at a time.

        Iterates over a list of JSON strings, sending each one in a separate POST request.

        Args:
            data (scoda_dataset.Dataset): The dataset to be uploaded, which should
                provide a `json_list_str` attribute containing
                individual JSON strings.
        """
        json_str: str
        for json_str in data.json_list_str:
            post(
                url=self.db_uri,
                auth=self.auth,
                headers=self.headers,
                data=json_str,
                timeout=600,
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
        super().__init__(
            uri="mongodb://localhost:27017",
            username="root",
            password="example",
            database="research",
        )
        self.client = MongoClient(self.uri)
        self.db = self.client[self.database]

    def create(self) -> None:
        """
        Creates a collection in the MongoDB database if it does not already exist.

        Checks for the existence of the collection and creates it if necessary.
        """
        try:
            self.db.create_collection(self.database)
        except CollectionInvalid:
            print("Collection already exists.")

    def recreate(self) -> None:
        """
        Deletes and recreates the collection in the MongoDB database.

        Drops the existing collection and then calls the `create` method to create a new collection.
        """
        self.db.drop_collection("research_data")
        self.create()

    def batch_upload(self, data: scoda_dataset.Dataset) -> None:
        """
        Uploads data to the MongoDB database in a single batch operation.

        Inserts the entire dataset into the collection.

        Args:
            data (scoda_dataset.Dataset): The dataset to be uploaded, which should
                                          provide a `json_list` attribute for serialization.
        """
        self.db["research_data"].insert_many(data.json_dict)

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
            self.db["research_data"].insert_one(document)

import os

import pandas as pd
import pymongo
import pymongo.collection
import pymongo.database
import pymongo.errors
import requests
import requests.auth

import scoda.datasets.generic
from scoda.db import RESPONSE_TIMEOUT
from scoda.db.document.generic import DocumentDB


class CouchDB(DocumentDB):
    def __init__(self) -> None:
        super().__init__(convert_time_column_to_int=False)
        self.uri: str = "http://localhost:5984/research"
        self.headers: dict[str, str] = {"Content-Type": "application/json"}
        self.auth: requests.auth.HTTPBasicAuth = requests.auth.HTTPBasicAuth(
            username=os.environ["COUCHDB_USERNAME"],
            password=os.environ["COUCHDB_PASSWORD"],
        )

        self.create()

    def batch_upload(self, dataset: scoda.datasets.generic.Dataset) -> None:
        """
        Upload data in batch to the database.

        This method iterates over a dataset and sends each record to the
        database.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` instance.

        """
        requests.post(
            url=f"{self.uri}/_bulk_docs",
            auth=self.auth,
            headers=self.headers,
            data='{"docs": ' + dataset.json_data_str + "}",
            timeout=RESPONSE_TIMEOUT,
        )

    def batch_read(self, table_name: str) -> None:
        """
        Perform a batch read of all metrics from the database.

        Arguments:
            table_name: The name of the table to read from.

        """
        table_name = table_name.lower()
        json_data: str = '{"include_docs": True}'
        requests.get(
            url=f"{self.uri}/_all_docs?include_docs=true",
            auth=self.auth,
            headers=self.headers,
            data=json_data,
            timeout=RESPONSE_TIMEOUT,
        )

    def create(self) -> None:
        resp: requests.Response = requests.get(
            url=self.uri,
            auth=self.auth,
            timeout=RESPONSE_TIMEOUT,
        )
        if resp.status_code != 200:
            requests.put(url=self.uri, auth=self.auth, timeout=RESPONSE_TIMEOUT)

    def delete(self) -> None:
        """
        Delete all data from the database.

        This method sends removes all stored data.

        """
        requests.delete(url=self.uri, auth=self.auth, timeout=RESPONSE_TIMEOUT)

    def query_average_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query the average value of a column in a given table.

        This method retrieves the average value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the average value.

        """
        resp: requests.Response = requests.get(
            url=f"{self.uri}/_all_docs?include_docs=true",
            auth=self.auth,
            headers=self.headers,
            timeout=RESPONSE_TIMEOUT,
        )

        docs: list[dict] = [x["doc"] for x in resp.json()["rows"]]
        data: pd.DataFrame = pd.DataFrame(data=docs)
        table: pd.DataFrame = data[data["name"] == table_name]
        column: pd.Series = table[column_name]
        column.mean()

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query a time window value grouped by a table name and column name.

        This function sends a request to group a specified column over an one
        hour time window.

        Arguments:
            table_name: The name of the table to group by.
            column_name: The name of the column to average over time.

        """
        resp: requests.Response = requests.get(
            url=f"{self.uri}/_all_docs?include_docs=true",
            auth=self.auth,
            headers=self.headers,
            timeout=RESPONSE_TIMEOUT,
        )

        docs: list[dict] = [x["doc"] for x in resp.json()["rows"]]
        data: pd.DataFrame = pd.DataFrame(data=docs)
        data[column_name] = pd.to_datetime(arg=data[column_name], utc=True)
        table: pd.DataFrame = data[data["name"] == table_name]
        table.groupby([table[column_name].dt.hour])

    def query_max_value(self, table_name: str, column_name: str) -> None:
        """
        Query the maximum value of a column in a given table.

        This method retrieves the maximum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the maximum value.

        """
        resp: requests.Response = requests.get(
            url=f"{self.uri}/_all_docs?include_docs=true",
            auth=self.auth,
            headers=self.headers,
            timeout=RESPONSE_TIMEOUT,
        )

        docs: list[dict] = [x["doc"] for x in resp.json()["rows"]]
        data: pd.DataFrame = pd.DataFrame(data=docs)
        table: pd.DataFrame = data[data["name"] == table_name]
        column: pd.Series = table[column_name]
        column.max()

    def query_min_value(self, table_name: str, column_name: str) -> None:
        """
        Query the minimum value of a column in a given table.

        This method retrieves the minimum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the minimum value.

        """
        resp: requests.Response = requests.get(
            url=f"{self.uri}/_all_docs?include_docs=true",
            auth=self.auth,
            headers=self.headers,
            timeout=RESPONSE_TIMEOUT,
        )

        docs: list[dict] = [x["doc"] for x in resp.json()["rows"]]
        data: pd.DataFrame = pd.DataFrame(data=docs)
        table: pd.DataFrame = data[data["name"] == table_name]
        column: pd.Series = table[column_name]
        column.min()

    def query_mode_value(self, table_name: str, column_name: str) -> None:
        """
        Query the mode value from a specified table and column.

        It is intended to support analysis for determining the most frequent
        (mode) value within the specified table and column.

        Arguments:
            table_name: The name of the table to query.
            column_name: The name of the column to extract the mode value from.

        """
        resp: requests.Response = requests.get(
            url=f"{self.uri}/_all_docs?include_docs=true",
            auth=self.auth,
            headers=self.headers,
            timeout=RESPONSE_TIMEOUT,
        )

        docs: list[dict] = [x["doc"] for x in resp.json()["rows"]]
        data: pd.DataFrame = pd.DataFrame(data=docs)
        table: pd.DataFrame = data[data["name"] == table_name]
        column: pd.Series = table[column_name]
        column.mode()

    def sequential_read(self, table_name: str, rows: int) -> None:
        """
        Perform a sequential read of records from a specified table.

        This method constructs and executes a  query to read all records from
        the specified table. It counts the number of records streamed from the
        query result but does not return or store them.

        Arguments:
            table_name: The name of the table (i.e., measurement) to read from.
            rows: The number of rows to read.

        """
        idx: int
        for idx in range(rows):
            json_body: str = '{"selector": {"id":' + str(idx) + '}, "limit": 1}'
            requests.post(
                url=f"{self.uri}/_find",
                auth=self.auth,
                headers=self.headers,
                data=json_body,
                timeout=RESPONSE_TIMEOUT,
            )

    def sequential_upload(
        self,
        dataset: scoda.datasets.generic.Dataset,
    ) -> None:
        """
        Upload data sequentially to the database.

        This method iterates over each row in a dataset and writes it
        individually to the database with a short delay between uploads. Each
        row is converted to a line protocol point with appropriate tags and
        fields. Invalid or missing field values are skipped, and non-numeric
        strings are cast to float where possible.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` containing time series data
                    with a time index and one or more value columns.

        """
        json_str: str
        for json_str in dataset.json_data_list:
            resp = requests.post(
                url=self.uri,
                auth=self.auth,
                headers=self.headers,
                data=json_str,
                timeout=RESPONSE_TIMEOUT,
            )


class MongoDB(DocumentDB):
    def __init__(self) -> None:
        super().__init__(convert_time_column_to_int=False)

        self.uri: str = "mongodb://root:example@localhost:27017"
        self.database_name: str = "research"
        self.collection_name: str = "research_data"

        self.database_conn: pymongo.database.Database
        self.collection_conn: pymongo.collection.Collection
        self.client: pymongo.MongoClient = pymongo.MongoClient(
            host=self.uri,
            tz_aware=True,
            connect=True,
        )

        self.create()

    def batch_upload(self, dataset: scoda.datasets.generic.Dataset) -> None:
        """
        Upload data in batch to the database.

        This method iterates over a dataset and sends each record to the
        database.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` instance.

        """
        self.collection_conn.insert_many(dataset.json_data)

    def batch_read(self, table_name: str) -> None:
        """
        Perform a batch read of all metrics from the database.

        Arguments:
            table_name: The name of the table to read from.

        """
        results = self.collection_conn.find({"name": table_name})
        pd.DataFrame(results)

    def create(self) -> None:
        self.database_conn = self.client[self.database_name]

        try:
            self.database_conn.create_collection(name=self.collection_name)
        except pymongo.errors.CollectionInvalid:
            pass

        self.collection_conn = self.database_conn[self.collection_name]

    def delete(self) -> None:
        """
        Delete all data from the database.

        This method sends removes all stored data.

        """
        self.database_conn.drop_collection(
            name_or_collection=self.collection_name,
        )
        self.client.drop_database(name_or_database=self.database_name)

    def query_average_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query the average value of a column in a given table.

        This method retrieves the average value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the average value.

        """
        pipeline = [
            {"$match": {"name": table_name}},
            {"$group": {"_id": None, "avg": {"$avg": f"${column_name}"}}},
        ]
        list(self.collection_conn.aggregate(pipeline))

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query a time window value grouped by a table name and column name.

        This function sends a request to group a specified column over an one
        hour time window.

        Arguments:
            table_name: The name of the table to group by.
            column_name: The name of the column to average over time.

        """
        docs = list(self.collection_conn.find({"name": table_name}))
        df = pd.DataFrame(docs)
        df[column_name] = pd.to_datetime(df[column_name], utc=True)
        df.groupby(df[column_name].dt.floor("h"))

    def query_max_value(self, table_name: str, column_name: str) -> None:
        """
        Query the maximum value of a column in a given table.

        This method retrieves the maximum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the maximum value.

        """
        pipeline = [
            {"$match": {"name": table_name}},
            {"$group": {"_id": None, "max": {"$max": f"${column_name}"}}},
        ]
        list(self.collection_conn.aggregate(pipeline))

    def query_min_value(self, table_name: str, column_name: str) -> None:
        """
        Query the minimum value of a column in a given table.

        This method retrieves the minimum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the minimum value.

        """
        pipeline = [
            {"$match": {"name": table_name}},
            {"$group": {"_id": None, "min": {"$min": f"${column_name}"}}},
        ]
        list(self.collection_conn.aggregate(pipeline))

    def query_mode_value(self, table_name: str, column_name: str) -> None:
        """
        Query the mode value from a specified table and column.

        It is intended to support analysis for determining the most frequent
        (mode) value within the specified table and column.

        Arguments:
            table_name: The name of the table to query.
            column_name: The name of the column to extract the mode value from.

        """
        pipeline = [
            {"$match": {"name": table_name}},
            {"$group": {"_id": f"${column_name}", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1},
        ]
        list(self.collection_conn.aggregate(pipeline))

    def sequential_read(self, table_name: str, rows: int) -> None:
        """
        Perform a sequential read of records from a specified table.

        This method constructs and executes a  query to read all records from
        the specified table. It counts the number of records streamed from the
        query result but does not return or store them.

        Arguments:
            table_name: The name of the table (i.e., measurement) to read from.
            rows: The number of rows to read.

        """
        cursor = self.collection_conn.find({"name": table_name})

        count: int = 0
        for doc in cursor:
            count += 1

    def sequential_upload(
        self,
        dataset: scoda.datasets.generic.Dataset,
    ) -> None:
        """
        Upload data sequentially to the database.

        This method iterates over each row in a dataset and writes it
        individually to the database with a short delay between uploads. Each
        row is converted to a line protocol point with appropriate tags and
        fields. Invalid or missing field values are skipped, and non-numeric
        strings are cast to float where possible.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` containing time series data
                    with a time index and one or more value columns.

        """
        for doc in dataset.json_data:
            self.collection_conn.insert_one(doc)

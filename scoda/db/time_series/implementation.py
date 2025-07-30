"""
Implementations of time-series databases.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import os
import time
from json import dumps

import pandas as pd
import requests
from influxdb_client.client.bucket_api import BucketsApi
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryApi
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import (
    SYNCHRONOUS,
    WriteApi,
    WriteOptions,
)
from influxdb_client.domain.bucket_retention_rules import BucketRetentionRules

import scoda.datasets.generic
import scoda.db.time_series.generic
from scoda.db import RESPONSE_TIMEOUT


class InfluxDB(scoda.db.time_series.generic.TimeSeriesDB):
    """
    Interface for interacting with an InfluxDB instance.

    This class initializes and manages InfluxDB client components for reading
    and writing time series data. Configuration parameters are loaded from
    environment variables: `INFLUXDB_TOKEN`, `INFLUXDB_BUCKET`, `INFLUXDB_ORG`,
    and `INFLUXDB_URI`.

    The class sets up the write clients (both batch and synchronous) and the
    buckets API for managing storage buckets. It also defines a default
    retention period.

    """

    def __init__(self) -> None:
        """
        Initialize an InfluxDB client for time series operations.

        This constructor loads configuration values from environment variables
        and sets up InfluxDB client components, including APIs for bucket
        management and data writing.

        Environment variables used:
            INFLUXDB_TOKEN: Authentication token for the InfluxDB instance.
            INFLUXDB_BUCKET: Target bucket for storing time series data.
            INFLUXDB_ORG: Organization associated with the InfluxDB account.
            INFLUXDB_URI: URI of the InfluxDB instance.

        Attributes:
            token: Authentication token for the InfluxDB client.
            bucket: Bucket name for storing time series data.
            org: Organization name.
            uri: Base URI for the InfluxDB instance.
            retention_seconds: Default retention period in seconds.
            bucket_api: API interface for bucket management.
            write_client: InfluxDB client instance for issuing operations.
            batch_write_api: Write API for asynchronous batch writing.
            write_api: Synchronous write API for reliable writes.

        """
        super().__init__()
        self.token: str = os.environ["INFLUXDB_TOKEN"]
        self.bucket: str = os.environ["INFLUXDB_BUCKET"]
        self.org: str = os.environ["INFLUXDB_ORG"]
        self.uri: str = os.environ["INFLUXDB_URI"]

        self.retention_seconds: int = 3153600000

        self.bucket_api: BucketsApi = InfluxDBClient(
            url=self.uri,
            token=self.token,
            org=self.org,
        ).buckets_api()

        self.write_client: InfluxDBClient = InfluxDBClient(
            url=self.uri,
            token=self.token,
            org=self.org,
        )

        self.batch_write_api: WriteApi = self.write_client.write_api()
        self.write_api: WriteApi = self.write_client.write_api(
            write_options=SYNCHRONOUS,
        )

    def batch_upload(self, dataset: scoda.datasets.generic.Dataset) -> None:
        """
        Upload data in batch to the database.

        This method iterates over a dataset and sends each record to the
        database.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` instance.

        """
        self.batch_write_api._write_options = WriteOptions(
            batch_size=dataset.time_series_data.shape[0],
        )

        self.batch_write_api.write(
            bucket=self.bucket,
            org=self.org,
            record=dataset.time_series_data,
            data_frame_measurement_name=dataset.name,
            data_frame_tag_columns=["name"],
        )

        time.sleep(0.1)

    def batch_read(self, table_name: str) -> None:
        """
        Perform a batch read of all metrics from the database.

        Arguments:
            table_name: The name of the table to read from.

        """
        query = f'''
        from(bucket: "{self.bucket}")
        |> range(start: 0)
        |> filter(fn: (r) => r["name"] == "{table_name}")
        '''  # noqa: Q001

        query_api: QueryApi = self.write_client.query_api()
        query_api.query_data_frame(query=query, org=self.org)

    def create(self) -> None:
        """Create the database if it does not already exist."""
        existing_buckets: list = self.bucket_api.find_buckets().buckets
        bucket_exists: bool = any(
            bucket.name == self.bucket for bucket in existing_buckets
        )

        if not bucket_exists:
            self.bucket_api.create_bucket(
                bucket_name=self.bucket,
                org=self.org,
                retention_rules=BucketRetentionRules(
                    type="expire",
                    every_seconds=self.retention_seconds,
                ),
            )

    def delete(self) -> None:
        """
        Delete all data from the database.

        This method sends removes all stored data.

        """
        bucket = self.bucket_api.find_bucket_by_name(self.bucket)
        if bucket is not None:
            self.bucket_api.delete_bucket(bucket)

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
        query = f'''
        from(bucket: "{self.bucket}")
        |> range(start: 0)
        |> filter(fn: (r) => r["name"] == "{table_name}")
        |> filter(fn: (r) => r["_field"] == "{column_name}")
        |> mean()
        '''  # noqa: Q001

        # Execute and fetch the average value
        query_api = self.write_client.query_api()
        query_api.query(query=query, org=self.org)

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
        query = f"""
        from(bucket: "{self.bucket}")
        |> range(start: 0)
        |> filter(fn: (r) => r["name"] == "{table_name}")
        |> filter(fn: (r) => r["_field"] == "{column_name}")
        |> group(columns: ["_time", "_value"])
        |> aggregateWindow(every: 1h, fn: count, createEmpty: false)
        |> group(columns: ["_time"])
        |> sort(columns: ["_value"], desc: true)
        |> limit(n: 1)
        """

        # Execute and fetch the average value
        query_api = self.write_client.query_api()
        query_api.query(query=query, org=self.org)

    def query_max_value(self, table_name: str, column_name: str) -> None:
        """
        Query the maximum value of a column in a given table.

        This method retrieves the maximum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the maximum value.

        """
        query = f'''
        from(bucket: "{self.bucket}")
        |> range(start: 0)
        |> filter(fn: (r) => r["name"] == "{table_name}")
        |> filter(fn: (r) => r["_field"] == "{column_name}")
        |> max()
        '''  # noqa: Q001

        # Execute and fetch the average value
        query_api = self.write_client.query_api()
        query_api.query(query=query, org=self.org)

    def query_min_value(self, table_name: str, column_name: str) -> None:
        """
        Query the minimum value of a column in a given table.

        This method retrieves the minimum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the minimum value.

        """
        query = f'''
        from(bucket: "{self.bucket}")
        |> range(start: 0)
        |> filter(fn: (r) => r["name"] == "{table_name}")
        |> filter(fn: (r) => r["_field"] == "{column_name}")
        |> min()
        '''  # noqa: Q001

        # Execute and fetch the average value
        query_api = self.write_client.query_api()
        query_api.query(query=query, org=self.org)

    def query_mode_value(self, table_name: str, column_name: str) -> None:
        """
        Query the mode value from a specified table and column.

        It is intended to support analysis for determining the most frequent
        (mode) value within the specified table and column.

        Arguments:
            table_name: The name of the table to query.
            column_name: The name of the column to extract the mode value from.

        """
        query = f'''
        from(bucket: "{self.bucket}")
        |> range(start: 0)
        |> filter(fn: (r) => r["name"] == "{table_name}")
        |> filter(fn: (r) => r["_field"] == "{column_name}")
        |> group(columns: ["_value"])
        |> count()
        |> group()
        |> sort(columns: ["_value"], desc: false)
        |> sort(columns: ["_value"], desc: true)
        |> limit(n: 1)
        '''  # noqa: Q001

        # Execute and fetch the average value
        query_api = self.write_client.query_api()
        query_api.query(query=query, org=self.org)

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
        rows += 1
        query = f'''
        from(bucket: "{self.bucket}")
        |> range(start: 0)
        |> filter(fn: (r) => r["name"] == "{table_name}")
        '''  # noqa: Q001
        query_api: QueryApi = self.write_client.query_api()

        count: int = 0
        for _ in query_api.query_stream(query=query, org=self.org):
            count += 1  # noqa: SIM113

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
        error_counter: int = 0
        for _, row in dataset.time_series_data.iterrows():
            timestamp = row.name
            point = Point(dataset.name).time(timestamp)

            # Add tags (e.g., name)
            point.tag("name", dataset.name)

            # Add fields
            for col in row.index:
                if col == dataset.time_column:
                    continue
                value = row[col]
                if pd.isna(value):
                    continue
                try:
                    value = (
                        float(value)
                        if isinstance(value, str)
                        and value.replace(".", "", 1).isdigit()
                        else value
                    )
                    point.field(col, value)
                except Exception:  # noqa: BLE001
                    error_counter += 1

            self.write_api.write(
                bucket=self.bucket,
                org=self.org,
                record=point,
            )

            time.sleep(0.1)


class VictoriaMetrics(scoda.db.time_series.generic.TimeSeriesDB):
    """
    VictoriaMetrics time series database client implementation.

    This class implements methods for reading, writing, deleting, and querying
    time series data in VictoriaMetrics using the HTTP API. It supports both
    batch and sequential operations and basic aggregation queries.

    """

    def __init__(self) -> None:
        """
        Initialize the VictoriaMetrics client with the default URI.

        This sets up the base URI for interacting with the VictoriaMetrics HTTP
        API. The default URI is 'http://localhost:8428'. nherits from the base
        class and sets up necessary attributes for further operations.

        """
        super().__init__()
        self.uri: str = os.environ["VICTORIAMETRICS_URI"]

    def create(self) -> None:
        """Create the database if it does not already exist."""
        pass  # noqa: PIE790

    def delete(self) -> None:
        """
        Delete all data from the database.

        This method sends removes all stored data.

        """
        # To delete all data, use the `delete_series` API
        try:
            requests.post(
                f"{self.uri}/api/v1/admin/tsdb/delete_series",
                params={"match[]": '{__name__=~".*"}'},
                timeout=RESPONSE_TIMEOUT,
            )
        except Exception:  # noqa: BLE001
            time.sleep(1)

    def batch_upload(self, dataset: scoda.datasets.generic.Dataset) -> None:
        """
        Upload data in batch to the database.

        This method iterates over a dataset and sends each record to the
        database.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` instance.

        """
        obj: dict
        for obj in dataset.victoriametric_json:
            json: str = dumps(obj=obj)
            try:
                requests.post(
                    f"{self.uri}/api/v1/import",
                    data=json,
                    timeout=RESPONSE_TIMEOUT,
                )
            except Exception:  # noqa: BLE001
                time.sleep(1)

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
        self.batch_upload(dataset)  # No real diff for VictoriaMetrics

    def batch_read(self, table_name: str) -> None:
        """
        Perform a batch read of all metrics from the database.

        Arguments:
            table_name: The name of the table to read from.

        """
        table_name = table_name.lower()
        requests.get(
            f"{self.uri}/api/v1/export",
            params={"match[]": '{__name__=~".*"}'},
            timeout=RESPONSE_TIMEOUT,
        )

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
        rows += 0
        self.batch_read(table_name)

    def query_average_value(self, table_name: str, column_name: str) -> None:
        """
        Query the average value of a column in a given table.

        This method retrieves the average value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the average value.

        """
        query = {
            "query": f'avg({column_name}{{name="{table_name}"}})',
            "start": "-1y",
        }
        requests.get(
            f"{self.uri}/api/v1/query",
            params=query,
            timeout=RESPONSE_TIMEOUT,
        )

    def query_min_value(self, table_name: str, column_name: str) -> None:
        """
        Query the minimum value of a column in a given table.

        This method retrieves the minimum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the minimum value.

        """
        query = {
            "query": f'min({column_name}{{name="{table_name}"}})',
            "start": "-1y",
        }
        requests.get(
            f"{self.uri}/api/v1/query",
            params=query,
            timeout=RESPONSE_TIMEOUT,
        )

    def query_max_value(self, table_name: str, column_name: str) -> None:
        """
        Query the maximum value of a column in a given table.

        This method retrieves the maximum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the maximum value.

        """
        query = {
            "query": f'max({column_name}{{name="{table_name}"}})',
            "start": "-1y",
        }
        requests.get(
            f"{self.uri}/api/v1/query",
            params=query,
            timeout=RESPONSE_TIMEOUT,
        )

    def query_mode_value(self, table_name: str, column_name: str) -> None:
        """
        Query the mode value from a specified table and column.

        It is intended to support analysis for determining the most frequent
        (mode) value within the specified table and column.

        Arguments:
            table_name: The name of the table to query.
            column_name: The name of the column to extract the mode value from.

        """
        table_name = table_name.lower()
        column_name = column_name.lower()

        requests.get(
            f"{self.uri}/api/v1/export",
            params={"match[]": '{__name__=~".*"}'},
            timeout=RESPONSE_TIMEOUT,
        )

    def query_groupby_time_window_value(
        self, table_name: str, column_name: str
    ) -> None:
        """
        Query a time window value grouped by a table name and column name.

        This function sends a request to group a specified column over an one
        hour time window.

        Arguments:
            table_name: The name of the table to group by.
            column_name: The name of the column to average over time.

        """
        query = {
            "query": f'avg_over_time({column_name}{{name="{table_name}"}}[{"1h"}])',
            "start": "-1y",
        }
        requests.get(
            f"{self.uri}/api/v1/query",
            params=query,
            timeout=RESPONSE_TIMEOUT,
        )

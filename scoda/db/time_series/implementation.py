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


class InfluxDB(scoda.db.time_series.generic.TimeSeriesDB):
    def __init__(self) -> None:
        super().__init__()
        self.token: str = "lsJ1cR9FBJojeZ-HGzN9kAdhc1XYcagfq8MkaKAmeMJP_Ux2hOOu3n-84aSNP0EaP_kIXGdaByl19MP6938WuA=="
        self.bucket: str = "research"
        self.org: str = "research"
        self.uri: str = "http://localhost:8086"

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
        query = f'''
        from(bucket: "{self.bucket}")
        |> range(start: 0)
        |> filter(fn: (r) => r["name"] == "{table_name}")
        '''

        query_api: QueryApi = self.write_client.query_api()
        query_api.query_data_frame(query=query, org=self.org)

    def create(self) -> None:
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
        bucket = self.bucket_api.find_bucket_by_name(self.bucket)
        if bucket is not None:
            self.bucket_api.delete_bucket(bucket)

    def query_average_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        query = f'''
        from(bucket: "{self.bucket}")
        |> range(start: 0)
        |> filter(fn: (r) => r["name"] == "{table_name}")
        |> filter(fn: (r) => r["_field"] == "{column_name}")
        |> mean()
        '''

        # Execute and fetch the average value
        query_api = self.write_client.query_api()
        query_api.query(query=query, org=self.org)

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
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
        query = f'''
        from(bucket: "{self.bucket}")
        |> range(start: 0)
        |> filter(fn: (r) => r["name"] == "{table_name}")
        |> filter(fn: (r) => r["_field"] == "{column_name}")
        |> max()
        '''

        # Execute and fetch the average value
        query_api = self.write_client.query_api()
        query_api.query(query=query, org=self.org)

    def query_min_value(self, table_name: str, column_name: str) -> None:
        query = f'''
        from(bucket: "{self.bucket}")
        |> range(start: 0)
        |> filter(fn: (r) => r["name"] == "{table_name}")
        |> filter(fn: (r) => r["_field"] == "{column_name}")
        |> min()
        '''

        # Execute and fetch the average value
        query_api = self.write_client.query_api()
        query_api.query(query=query, org=self.org)

    def query_mode_value(self, table_name: str, column_name: str) -> None:
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
        '''

        # Execute and fetch the average value
        query_api = self.write_client.query_api()
        query_api.query(query=query, org=self.org)

    def sequential_read(self, table_name: str, rows: int) -> None:
        query = f'''
        from(bucket: "{self.bucket}")
        |> range(start: 0)
        |> filter(fn: (r) => r["name"] == "{table_name}")
        '''
        query_api: QueryApi = self.write_client.query_api()

        count: int = 0
        for _ in query_api.query_stream(query=query, org=self.org):
            count += 1

    def sequential_upload(self, dataset: scoda.datasets.generic.Dataset) -> None:
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
                except Exception as e:
                    pass

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)

            time.sleep(0.1)


class VictoriaMetrics(scoda.db.time_series.generic.TimeSeriesDB):
    def __init__(self) -> None:
        super().__init__()
        self.uri: str = "http://localhost:8428"

    def create(self) -> None:
        pass  # No-op for VictoriaMetrics

    def delete(self) -> None:
        # To delete all data, use the `delete_series` API
        requests.post(
            f"{self.uri}/api/v1/admin/tsdb/delete_series",
            params={"match[]": '{__name__=~".*"}'},
        )

    def batch_upload(self, dataset: scoda.datasets.generic.Dataset) -> None:
        obj: dict
        try:
            for obj in dataset.victoriametric_json:
                json: str = dumps(obj=obj)
                requests.post(f"{self.uri}/api/v1/import", data=json)
        except TypeError:
            print(dataset.name)
            quit()

    def sequential_upload(self, dataset: scoda.datasets.generic.Dataset) -> None:
        self.batch_upload(dataset)  # No real diff for VictoriaMetrics

    def batch_read(self, table_name: str) -> None:
        response = requests.get(
            f"{self.uri}/api/v1/export",
            params={"match[]": '{__name__=~".*"}'},
        )

    def sequential_read(self, table_name: str, rows: int) -> None:
        self.batch_read(table_name)

    def query_average_value(self, table_name: str, column_name: str) -> None:
        query = {"query": f'avg({column_name}{{name="{table_name}"}})', "start": "-1y"}
        requests.get(f"{self.uri}/api/v1/query", params=query)

    def query_min_value(self, table_name: str, column_name: str) -> None:
        query = {"query": f'min({column_name}{{name="{table_name}"}})', "start": "-1y"}
        r = requests.get(f"{self.uri}/api/v1/query", params=query)

    def query_max_value(self, table_name: str, column_name: str) -> None:
        query = {"query": f'max({column_name}{{name="{table_name}"}})', "start": "-1y"}
        r = requests.get(f"{self.uri}/api/v1/query", params=query)
        r.raise_for_status()

    def query_mode_value(self, table_name: str, column_name: str) -> None:
        requests.get(
            f"{self.uri}/api/v1/export",
            params={"match[]": '{__name__=~".*"}'},
        )

    def query_groupby_time_window_value(
        self, table_name: str, column_name: str
    ) -> None:
        query = {
            "query": f'avg_over_time({column_name}{{name="{table_name}"}}[{"1h"}])',
            "start": "-1y",
        }
        requests.get(f"{self.uri}/api/v1/query", params=query)

import time

import pandas as pd
from influxdb_client.client.bucket_api import BucketsApi
from influxdb_client.client.influxdb_client import InfluxDBClient
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
        self.token: str = "2_k8KvLd_ehKs7DiSZzHj3XhBXJZpetNMInWLBH0H3q6S7J5gEpqgFvaUFcCWp926cu3qBBRhi_BVXfnt9D3NA"
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

    def batch_read(self, table_name: str) -> None: ...

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
    ) -> None: ...

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None: ...

    def query_max_value(self, table_name: str, column_name: str) -> None: ...

    def query_min_value(self, table_name: str, column_name: str) -> None: ...

    def query_mode_value(self, table_name: str, column_name: str) -> None: ...

    def sequential_read(self, table_name: str, rows: int) -> None: ...

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

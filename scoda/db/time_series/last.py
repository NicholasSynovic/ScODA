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

import scoda.db.time_series as scoda_time_series
from scoda.datasets import Dataset


class LAST(scoda_time_series.TimeSeriesDB):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str = "research",
    ):
        super().__init__(uri, username, password, database)


class InfluxDB(LAST):
    def __init__(self):
        self.token: str = "2_k8KvLd_ehKs7DiSZzHj3XhBXJZpetNMInWLBH0H3q6S7J5gEpqgFvaUFcCWp926cu3qBBRhi_BVXfnt9D3NA=="
        self.bucket: str = "research"
        self.org: str = "research"

        uri: str = "http://localhost:8086"

        self.write_client: InfluxDBClient = InfluxDBClient(
            url=uri,
            token=self.token,
            org=self.org,
        )

        self.write_api: WriteApi = self.write_client.write_api(
            write_options=SYNCHRONOUS
        )
        self.batch_write_api: WriteApi = self.write_client.write_api()

        self.bucket_api: BucketsApi = InfluxDBClient(
            url=uri,
            token=self.token,
            org=self.org,
        ).buckets_api()

        super().__init__(
            uri="http://localhost:8086",
            username="root",
            password="example123",
            database="research",
        )

    def create(self) -> None:
        bucket = self.bucket_api.find_bucket_by_name(self.bucket)
        if bucket is not None:
            self.bucket_api.delete_bucket(bucket)

        retention_seconds = 100 * 365 * 24 * 60 * 60

        existing_buckets = self.bucket_api.find_buckets().buckets
        bucket_exists = any(bucket.name == self.bucket for bucket in existing_buckets)

        if not bucket_exists:
            self.bucket_api.create_bucket(
                bucket_name=self.bucket,
                org=self.org,
                retention_rules=BucketRetentionRules(
                    type="expire", every_seconds=retention_seconds
                ),
            )

    def recreate(self) -> None:
        bucket = self.bucket_api.find_bucket_by_name(self.bucket)
        self.bucket_api.delete_bucket(bucket)
        self.create()

    def batch_upload(self, data: Dataset) -> None:
        self.batch_write_api._write_options = WriteOptions(
            batch_size=data.time_series_data.shape[0],
        )

        self.batch_write_api.write(
            bucket=self.bucket,
            org=self.org,
            record=data.time_series_data,
            data_frame_measurement_name=data.name,
            data_frame_tag_columns=["name"],
        )

        time.sleep(0.1)

    def sequential_upload(self, data: Dataset) -> None:
        for idx, row in data.time_series_data.iterrows():
            timestamp = row.name
            point = Point(data.name).time(timestamp)

            # Add tags (e.g., name)
            point.tag("name", data.name)

            # Add fields
            for col in row.index:
                if col == data.time_column:
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

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.bucket_api import BucketsApi
from influxdb_client.client.write_api import SYNCHRONOUS, WriteApi
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
        self.token: str = "research_token"
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
        retention_seconds = 30 * 24 * 60 * 60  # 30 days

        existing_buckets = self.bucket_api.find_buckets().buckets
        bucket_exists = any(bucket.name == self.bucket for bucket in existing_buckets)

        if not bucket_exists:
            bucket = self.bucket_api.create_bucket(
                bucket_name=self.bucket,
                org=self.org,
                retention_rules=BucketRetentionRules(
                    type="expire", every_seconds=retention_seconds
                ),
            )

    def recreate(self) -> None:
        self.bucket_api.delete_bucket(self.bucket)
        self.create()

    def batch_upload(self, data: Dataset) -> None:
        self.write_api.write(
            bucket=self.bucket,
            org=self.org,
            record=data.time_series_data,
            data_frame_measurement_name=data.name,
            data_frame_tag_columns=["name"],  # tag column(s)
        )

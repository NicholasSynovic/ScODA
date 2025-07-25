import time
from typing import Optional

import pandas as pd
import requests
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


class VictoriaMetrics(LAST):
    def __init__(
        self,
        uri: str = "http://localhost:8428",
        username: str = "",
        password: str = "",
        database: str = "research",
    ):
        self.uri = uri
        self.database = database
        self.auth: Optional[tuple] = (
            (username, password) if username and password else None
        )
        self.write_url = f"{self.uri}/write"
        super().__init__(uri, username, password, database)

    def create(self) -> None:
        pass

    def recreate(self) -> None:
        pass

    def _send_lines(self, lines: list[str]) -> None:
        if not lines:
            print("⚠️ No data to write.")
            return

        payload = "\n".join(lines)
        response = requests.post(
            self.write_url,
            params={"db": self.database},
            data=payload.encode("utf-8"),
            auth=self.auth,
            headers={"Content-Type": "text/plain"},
        )
        if not response.ok:
            raise RuntimeError(
                f"VictoriaMetrics write failed: {response.status_code} {response.text}"
            )

    def batch_upload(self, data: Dataset) -> None:
        df = data.time_series_data
        lines = []

        for timestamp, row in df.iterrows():
            point = Point(data.name).time(timestamp)
            point.tag("name", data.name)

            for col in row.index:
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
                except Exception:
                    continue

            lines.append(point.to_line_protocol())

        self._send_lines(lines)

    def sequential_upload(self, data: Dataset) -> None:
        df = data.time_series_data

        for timestamp, row in df.iterrows():
            point = Point(data.name).time(timestamp)
            point.tag("name", data.name)

            for col in row.index:
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
                except Exception:
                    continue

            line = point.to_line_protocol()
            self._send_lines([line])

from scoda.api.db.llnl_last import LLNL_LAST
from scoda.api.db import DocumentDB
from scoda.api.db.theta import Theta
from pathlib import Path
from sqlalchemy import Table, Column, Float, Integer
from requests import get, put, post, Response
from requests.auth import HTTPBasicAuth
from typing import Any
import json


class PostgreSQL_LLNL(LLNL_LAST):
    def __init__(self) -> None:
        super().__init__(
            uri="postgresql+psycopg2://admin:example@localhost:5432/research"
        )


class PostgreSQL_Theta(Theta):
    def __init__(self) -> None:
        super().__init__(
            uri="postgresql+psycopg2://admin:example@localhost:5432/research"
        )


class MySQL_LLNL(LLNL_LAST):
    def __init__(self) -> None:
        super().__init__(uri="mysql+pymysql://root:example@localhost:3306/research")


class MySQL_Theta(Theta):
    def __init__(self) -> None:
        super().__init__(uri="mysql+pymysql://root:example@localhost:3306/research")


class MariaDB_LLNL(LLNL_LAST):
    def __init__(self) -> None:
        super().__init__(uri="mariadb+pymysql://root:example@localhost:3306/research")


class MariaDB_Theta(Theta):
    def __init__(self) -> None:
        super().__init__(uri="mariadb+pymysql://root:example@localhost:3306/research")


class DB2_LLNL(LLNL_LAST):
    def __init__(self) -> None:
        super().__init__(uri="db2+ibm_db://db2inst1:example@localhost:50000/research")


class DB2_Theta(Theta):
    def __init__(self) -> None:
        super().__init__(uri="db2+ibm_db://db2inst1:example@localhost:50000/research")


class SQLite3_LLNL(LLNL_LAST):
    def __init__(self, fp: Path) -> None:
        self.fp: Path = fp.resolve()
        super().__init__(uri=f"sqlite:///{self.fp}")


class SQLite3_Theta(Theta):
    def __init__(self, fp: Path) -> None:
        self.fp: Path = fp.resolve()
        super().__init__(uri=f"sqlite:///{self.fp}")


class InMemorySQLite3_LLNL(LLNL_LAST):
    def __init__(self) -> None:
        super().__init__(uri=f"sqlite:///:memory:")


class InMemorySQLite3_Theta(Theta):
    def __init__(self) -> None:
        super().__init__(uri=f"sqlite:///:memory:")


class Redis:
    # TODO: Implement this
    ...


class Valkey:
    # TODO: Implement this
    ...


class Druid:
    # TODO: implement this
    # https://projects.apache.org/project.html?druid
    ...


class Phoenix:
    # TODO: implement this
    # https://projects.apache.org/project.html?phoenix
    ...


class ElasticSearch:
    # TODO: implement this
    ...


class InfluxDB:
    # TODO: implement this
    ...


class MongoDB:
    # TODO: implement this
    ...


class CouchDB(DocumentDB):
    def __init__(self) -> None:
        super().__init__(
            url="http://localhost:5984",
            username="root",
            password="example",
        )
        self.db_url: str = self.url + "/" + self.database_name
        self.headers: dict[str, str] = {"Content-Type": "application/json"}
        self.auth: HTTPBasicAuth = HTTPBasicAuth(
            username=self.username,
            password=self.password,
        )

    def create_database(self) -> None:
        if get(url=self.db_url, auth=self.auth).status_code == 200:
            pass
        else:
            put(url=self.db_url, auth=self.auth)

    def upload(self, data: str) -> Response:
        return post(
            url=self.db_url,
            auth=self.auth,
            headers=self.headers,
            json=data,
        )

    def query_avg_value(self) -> None:
        pass

    def query_min_value(self) -> None:
        pass


class BenchmarkResults_Theta(SQLite3_Theta):
    def __init__(self, fp: Path) -> None:
        super().__init__(fp=fp)

    def create_tables(self) -> None:
        pass


class BenchmarkResults_LLNL(SQLite3_LLNL):
    def __init__(self, fp: Path) -> None:
        super().__init__(fp=fp)

    def create_tables(self) -> None:
        _: Table = Table(
            "benchmark_total_time_to_batch_write_tables",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("seconds", Float),
        )

        _: Table = Table(
            "benchmark_total_time_to_sequential_write_tables",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("seconds", Float),
        )

        _: Table = Table(
            "benchmark_min_query_on_all_tables",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("seconds", Float),
        )

        _: Table = Table(
            "benchmark_min_query_on_each_table",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("cori_power_30_sec", Float),
            Column("hawk_power_15_min", Float),
            Column("lumi_power_10_min", Float),
            Column("marconi100_power_60_sec", Float),
            Column("perlmutter_power_60_sec", Float),
            Column("lumi_hpcg", Float),
            Column("hpcg_dpc", Float),
            Column("hpcg_spc", Float),
            Column("hpcg_uc", Float),
            Column("hpl_dpc", Float),
            Column("hpl_spc", Float),
            Column("hpl_uc", Float),
        )

        _: Table = Table(
            "benchmark_total_time_to_batch_write_individual_tables",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("cori_power_30_sec", Float),
            Column("hawk_power_15_min", Float),
            Column("lumi_power_10_min", Float),
            Column("marconi100_power_60_sec", Float),
            Column("perlmutter_power_60_sec", Float),
            Column("lumi_hpcg", Float),
            Column("hpcg_dpc", Float),
            Column("hpcg_spc", Float),
            Column("hpcg_uc", Float),
            Column("hpl_dpc", Float),
            Column("hpl_spc", Float),
            Column("hpl_uc", Float),
        )

        _: Table = Table(
            "benchmark_total_time_to_sequential_write_individual_tables",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("cori_power_30_sec", Float),
            Column("hawk_power_15_min", Float),
            Column("lumi_power_10_min", Float),
            Column("marconi100_power_60_sec", Float),
            Column("perlmutter_power_60_sec", Float),
            Column("lumi_hpcg", Float),
            Column("hpcg_dpc", Float),
            Column("hpcg_spc", Float),
            Column("hpcg_uc", Float),
            Column("hpl_dpc", Float),
            Column("hpl_spc", Float),
            Column("hpl_uc", Float),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)

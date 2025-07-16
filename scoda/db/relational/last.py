from pathlib import Path

from sqlalchemy import Column, DateTime, Integer, Table

import scoda.db.relational as scoda_relational


class LAST(scoda_relational.RDBMS):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str = "research",
    ):
        super().__init__(uri, username, password, database)

    def create(self) -> None:
        _: Table = Table(
            "cori_power_30_sec",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("timestamp_secs", Integer),
            Column("measured_kW", Integer),
        )

        _: Table = Table(
            "hawk_power_15_min",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("timestamp_secs", Integer),
            Column("measured_kW", Integer),
        )

        _: Table = Table(
            "lumi_power_10_min",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("timestamp_secs", Integer),
            Column("measured_kW", Integer),
        )

        _: Table = Table(
            "marconi100_power_60_sec",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("timestamp_secs", Integer),
            Column("measured_kW", Integer),
        )

        _: Table = Table(
            "perlmutter_power_60_sec",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("timestamp_secs", Integer),
            Column("measured_kW", Integer),
        )

        _: Table = Table(
            "lumi_hpcg",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("timestamp_secs", Integer),
            Column("measured_kW", Integer),
        )

        # hpcg_dpc
        _: Table = Table(
            "hpcg_dpc",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("Time", DateTime),
            Column("hsmp", Integer),
            Column("Node_r9c1t1n1", Integer),
            Column("Node_r9c1t1n2", Integer),
            Column("Node_r9c1t1n3", Integer),
            Column("Node_r9c1t1n4", Integer),
            Column("Node_r9c1t2n1", Integer),
            Column("Node_r9c1t2n2", Integer),
            Column("Node_r9c1t2n3", Integer),
            Column("Node_r9c1t2n4", Integer),
            Column("Node_r9c1t3n1", Integer),
            Column("Node_r9c1t3n2", Integer),
            Column("Node_r9c1t3n3", Integer),
            Column("Node_r9c1t3n4", Integer),
            Column("Node_r9c1t4n1", Integer),
            Column("Node_r9c1t4n2", Integer),
            Column("Node_r9c1t4n3", Integer),
            Column("Node_r9c1t4n4", Integer),
            Column("Node_r9c1t5n1", Integer),
            Column("Node_r9c1t5n2", Integer),
            Column("Node_r9c1t5n3", Integer),
            Column("Node_r9c1t5n4", Integer),
            Column("Node_r9c1t6n1", Integer),
            Column("Node_r9c1t6n2", Integer),
            Column("Node_r9c1t6n3", Integer),
            Column("Node_r9c1t6n4", Integer),
            Column("Node_r9c1t7n1", Integer),
            Column("Node_r9c1t7n2", Integer),
            Column("Node_r9c1t7n3", Integer),
            Column("Node_r9c1t7n4", Integer),
            Column("Node_r9c1t8n1", Integer),
            Column("Node_r9c1t8n2", Integer),
            Column("Node_r9c1t8n3", Integer),
            Column("Node_r9c1t8n4", Integer),
            Column("Node_r9c2t1n1", Integer),
            Column("Node_r9c2t1n2", Integer),
            Column("Node_r9c2t1n3", Integer),
            Column("Node_r9c2t1n4", Integer),
            Column("Node_r9c2t2n1", Integer),
            Column("Node_r9c2t2n2", Integer),
            Column("Node_r9c2t2n3", Integer),
            Column("Node_r9c2t2n4", Integer),
            Column("Node_r9c2t3n1", Integer),
            Column("Node_r9c2t3n2", Integer),
            Column("Node_r9c2t3n3", Integer),
            Column("Node_r9c2t3n4", Integer),
            Column("Node_r9c2t4n1", Integer),
            Column("Node_r9c2t4n2", Integer),
            Column("Node_r9c2t4n3", Integer),
            Column("Node_r9c2t4n4", Integer),
            Column("Node_r9c2t5n1", Integer),
            Column("Node_r9c2t5n2", Integer),
            Column("Node_r9c2t5n3", Integer),
            Column("Node_r9c2t5n4", Integer),
            Column("Node_r9c2t6n1", Integer),
            Column("Node_r9c2t6n2", Integer),
            Column("Node_r9c2t6n3", Integer),
            Column("Node_r9c2t6n4", Integer),
            Column("Node_r9c2t7n1", Integer),
            Column("Node_r9c2t7n2", Integer),
            Column("Node_r9c2t7n3", Integer),
            Column("Node_r9c2t7n4", Integer),
            Column("Node_r9c2t8n1", Integer),
            Column("Node_r9c2t8n2", Integer),
            Column("Node_r9c2t8n3", Integer),
            Column("Node_r9c2t8n4", Integer),
        )

        # hpcg_spc
        _: Table = Table(
            "hpcg_spc",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("Time", DateTime),
            Column("hsmp", Integer),
            Column("Node_r6c3t1n1", Integer),
            Column("Node_r6c3t1n2", Integer),
            Column("Node_r6c3t1n3", Integer),
            Column("Node_r6c3t1n4", Integer),
            Column("Node_r6c3t2n1", Integer),
            Column("Node_r6c3t2n2", Integer),
            Column("Node_r6c3t2n3", Integer),
            Column("Node_r6c3t2n4", Integer),
            Column("Node_r6c3t3n1", Integer),
            Column("Node_r6c3t3n2", Integer),
            Column("Node_r6c3t3n3", Integer),
            Column("Node_r6c3t3n4", Integer),
            Column("Node_r6c3t4n1", Integer),
            Column("Node_r6c3t4n2", Integer),
            Column("Node_r6c3t4n3", Integer),
            Column("Node_r6c3t4n4", Integer),
            Column("Node_r6c3t5n1", Integer),
            Column("Node_r6c3t5n2", Integer),
            Column("Node_r6c3t5n3", Integer),
            Column("Node_r6c3t5n4", Integer),
            Column("Node_r6c3t6n1", Integer),
            Column("Node_r6c3t6n2", Integer),
            Column("Node_r6c3t6n3", Integer),
            Column("Node_r6c3t6n4", Integer),
            Column("Node_r6c3t7n1", Integer),
            Column("Node_r6c3t7n2", Integer),
            Column("Node_r6c3t7n3", Integer),
            Column("Node_r6c3t7n4", Integer),
            Column("Node_r6c3t8n1", Integer),
            Column("Node_r6c3t8n2", Integer),
            Column("Node_r6c3t8n3", Integer),
            Column("Node_r6c3t8n4", Integer),
            Column("Node_r6c4t1n1", Integer),
            Column("Node_r6c4t1n2", Integer),
            Column("Node_r6c4t1n3", Integer),
            Column("Node_r6c4t1n4", Integer),
            Column("Node_r6c4t2n1", Integer),
            Column("Node_r6c4t2n2", Integer),
            Column("Node_r6c4t2n3", Integer),
            Column("Node_r6c4t2n4", Integer),
            Column("Node_r6c4t3n1", Integer),
            Column("Node_r6c4t3n2", Integer),
            Column("Node_r6c4t3n3", Integer),
            Column("Node_r6c4t3n4", Integer),
            Column("Node_r6c4t4n1", Integer),
            Column("Node_r6c4t4n2", Integer),
            Column("Node_r6c4t4n3", Integer),
            Column("Node_r6c4t4n4", Integer),
            Column("Node_r6c4t5n1", Integer),
            Column("Node_r6c4t5n2", Integer),
            Column("Node_r6c4t5n3", Integer),
            Column("Node_r6c4t5n4", Integer),
            Column("Node_r6c4t6n1", Integer),
            Column("Node_r6c4t6n2", Integer),
            Column("Node_r6c4t6n3", Integer),
            Column("Node_r6c4t6n4", Integer),
            Column("Node_r6c4t7n1", Integer),
            Column("Node_r6c4t7n2", Integer),
            Column("Node_r6c4t7n3", Integer),
            Column("Node_r6c4t7n4", Integer),
            Column("Node_r6c4t8n1", Integer),
            Column("Node_r6c4t8n2", Integer),
            Column("Node_r6c4t8n3", Integer),
            Column("Node_r6c4t8n4", Integer),
        )

        # hpcg_uc
        _: Table = Table(
            "hpcg_uc",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("Time", DateTime),
            Column("hsmp", Integer),
            Column("Node_r7c3t1n1", Integer),
            Column("Node_r7c3t1n2", Integer),
            Column("Node_r7c3t1n3", Integer),
            Column("Node_r7c3t1n4", Integer),
            Column("Node_r7c3t2n1", Integer),
            Column("Node_r7c3t2n2", Integer),
            Column("Node_r7c3t2n3", Integer),
            Column("Node_r7c3t2n4", Integer),
            Column("Node_r7c3t3n1", Integer),
            Column("Node_r7c3t3n2", Integer),
            Column("Node_r7c3t3n3", Integer),
            Column("Node_r7c3t3n4", Integer),
            Column("Node_r7c3t4n1", Integer),
            Column("Node_r7c3t4n2", Integer),
            Column("Node_r7c3t4n3", Integer),
            Column("Node_r7c3t4n4", Integer),
            Column("Node_r7c3t5n1", Integer),
            Column("Node_r7c3t5n2", Integer),
            Column("Node_r7c3t5n3", Integer),
            Column("Node_r7c3t5n4", Integer),
            Column("Node_r7c3t6n1", Integer),
            Column("Node_r7c3t6n2", Integer),
            Column("Node_r7c3t6n3", Integer),
            Column("Node_r7c3t6n4", Integer),
            Column("Node_r7c3t7n1", Integer),
            Column("Node_r7c3t7n2", Integer),
            Column("Node_r7c3t7n3", Integer),
            Column("Node_r7c3t7n4", Integer),
            Column("Node_r7c3t8n1", Integer),
            Column("Node_r7c3t8n2", Integer),
            Column("Node_r7c3t8n3", Integer),
            Column("Node_r7c3t8n4", Integer),
            Column("Node_r7c4t1n1", Integer),
            Column("Node_r7c4t1n2", Integer),
            Column("Node_r7c4t1n3", Integer),
            Column("Node_r7c4t1n4", Integer),
            Column("Node_r7c4t2n1", Integer),
            Column("Node_r7c4t2n2", Integer),
            Column("Node_r7c4t2n3", Integer),
            Column("Node_r7c4t2n4", Integer),
            Column("Node_r7c4t3n1", Integer),
            Column("Node_r7c4t3n2", Integer),
            Column("Node_r7c4t3n3", Integer),
            Column("Node_r7c4t3n4", Integer),
            Column("Node_r7c4t4n1", Integer),
            Column("Node_r7c4t4n2", Integer),
            Column("Node_r7c4t4n3", Integer),
            Column("Node_r7c4t4n4", Integer),
            Column("Node_r7c4t5n1", Integer),
            Column("Node_r7c4t5n2", Integer),
            Column("Node_r7c4t5n3", Integer),
            Column("Node_r7c4t5n4", Integer),
            Column("Node_r7c4t6n1", Integer),
            Column("Node_r7c4t6n2", Integer),
            Column("Node_r7c4t6n3", Integer),
            Column("Node_r7c4t6n4", Integer),
            Column("Node_r7c4t7n1", Integer),
            Column("Node_r7c4t7n2", Integer),
            Column("Node_r7c4t7n3", Integer),
            Column("Node_r7c4t7n4", Integer),
            Column("Node_r7c4t8n1", Integer),
            Column("Node_r7c4t8n2", Integer),
            Column("Node_r7c4t8n3", Integer),
            Column("Node_r7c4t8n4", Integer),
        )

        # hpl_dpc
        _: Table = Table(
            "hpl_dpc",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("Time", DateTime),
            Column("hsmp", Integer),
            Column("Node_r10c1t1n1", Integer),
            Column("Node_r10c1t1n2", Integer),
            Column("Node_r10c1t1n3", Integer),
            Column("Node_r10c1t1n4", Integer),
            Column("Node_r10c1t2n1", Integer),
            Column("Node_r10c1t2n2", Integer),
            Column("Node_r10c1t2n3", Integer),
            Column("Node_r10c1t2n4", Integer),
            Column("Node_r10c1t3n1", Integer),
            Column("Node_r10c1t3n2", Integer),
            Column("Node_r10c1t3n3", Integer),
            Column("Node_r10c1t3n4", Integer),
            Column("Node_r10c1t4n1", Integer),
            Column("Node_r10c1t4n2", Integer),
            Column("Node_r10c1t4n3", Integer),
            Column("Node_r10c1t4n4", Integer),
            Column("Node_r10c1t5n1", Integer),
            Column("Node_r10c1t5n2", Integer),
            Column("Node_r10c1t5n3", Integer),
            Column("Node_r10c1t5n4", Integer),
            Column("Node_r10c1t6n1", Integer),
            Column("Node_r10c1t6n2", Integer),
            Column("Node_r10c1t6n3", Integer),
            Column("Node_r10c1t6n4", Integer),
            Column("Node_r10c1t7n1", Integer),
            Column("Node_r10c1t7n2", Integer),
            Column("Node_r10c1t7n3", Integer),
            Column("Node_r10c1t7n4", Integer),
            Column("Node_r10c1t8n1", Integer),
            Column("Node_r10c1t8n2", Integer),
            Column("Node_r10c1t8n3", Integer),
            Column("Node_r10c1t8n4", Integer),
            Column("Node_r10c2t1n1", Integer),
            Column("Node_r10c2t1n2", Integer),
            Column("Node_r10c2t1n3", Integer),
            Column("Node_r10c2t1n4", Integer),
            Column("Node_r10c2t2n1", Integer),
            Column("Node_r10c2t2n2", Integer),
            Column("Node_r10c2t2n3", Integer),
            Column("Node_r10c2t2n4", Integer),
            Column("Node_r10c2t3n1", Integer),
            Column("Node_r10c2t3n2", Integer),
            Column("Node_r10c2t3n3", Integer),
            Column("Node_r10c2t3n4", Integer),
            Column("Node_r10c2t4n1", Integer),
            Column("Node_r10c2t4n2", Integer),
            Column("Node_r10c2t4n3", Integer),
            Column("Node_r10c2t4n4", Integer),
            Column("Node_r10c2t5n1", Integer),
            Column("Node_r10c2t5n2", Integer),
            Column("Node_r10c2t5n3", Integer),
            Column("Node_r10c2t5n4", Integer),
            Column("Node_r10c2t6n1", Integer),
            Column("Node_r10c2t6n2", Integer),
            Column("Node_r10c2t6n3", Integer),
            Column("Node_r10c2t6n4", Integer),
            Column("Node_r10c2t7n1", Integer),
            Column("Node_r10c2t7n2", Integer),
            Column("Node_r10c2t7n3", Integer),
            Column("Node_r10c2t7n4", Integer),
            Column("Node_r10c2t8n1", Integer),
            Column("Node_r10c2t8n2", Integer),
            Column("Node_r10c2t8n3", Integer),
            Column("Node_r10c2t8n4", Integer),
        )

        # hpl_spc
        _: Table = Table(
            "hpl_spc",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("hsmp", Integer),
            Column("Time", DateTime),
            Column("Node_r14c3t1n1", Integer),
            Column("Node_r14c3t1n2", Integer),
            Column("Node_r14c3t1n3", Integer),
            Column("Node_r14c3t1n4", Integer),
            Column("Node_r14c3t2n1", Integer),
            Column("Node_r14c3t2n2", Integer),
            Column("Node_r14c3t2n3", Integer),
            Column("Node_r14c3t2n4", Integer),
            Column("Node_r14c3t3n1", Integer),
            Column("Node_r14c3t3n2", Integer),
            Column("Node_r14c3t3n3", Integer),
            Column("Node_r14c3t3n4", Integer),
            Column("Node_r14c3t4n1", Integer),
            Column("Node_r14c3t4n2", Integer),
            Column("Node_r14c3t4n3", Integer),
            Column("Node_r14c3t4n4", Integer),
            Column("Node_r14c3t5n1", Integer),
            Column("Node_r14c3t5n2", Integer),
            Column("Node_r14c3t5n3", Integer),
            Column("Node_r14c3t5n4", Integer),
            Column("Node_r14c3t6n1", Integer),
            Column("Node_r14c3t6n2", Integer),
            Column("Node_r14c3t6n3", Integer),
            Column("Node_r14c3t6n4", Integer),
            Column("Node_r14c3t7n1", Integer),
            Column("Node_r14c3t7n2", Integer),
            Column("Node_r14c3t7n3", Integer),
            Column("Node_r14c3t7n4", Integer),
            Column("Node_r14c3t8n1", Integer),
            Column("Node_r14c3t8n2", Integer),
            Column("Node_r14c3t8n3", Integer),
            Column("Node_r14c3t8n4", Integer),
            Column("Node_r14c4t1n1", Integer),
            Column("Node_r14c4t1n2", Integer),
            Column("Node_r14c4t1n3", Integer),
            Column("Node_r14c4t1n4", Integer),
            Column("Node_r14c4t2n1", Integer),
            Column("Node_r14c4t2n2", Integer),
            Column("Node_r14c4t2n3", Integer),
            Column("Node_r14c4t2n4", Integer),
            Column("Node_r14c4t3n1", Integer),
            Column("Node_r14c4t3n2", Integer),
            Column("Node_r14c4t3n3", Integer),
            Column("Node_r14c4t3n4", Integer),
            Column("Node_r14c4t4n1", Integer),
            Column("Node_r14c4t4n2", Integer),
            Column("Node_r14c4t4n3", Integer),
            Column("Node_r14c4t4n4", Integer),
            Column("Node_r14c4t5n1", Integer),
            Column("Node_r14c4t5n2", Integer),
            Column("Node_r14c4t5n3", Integer),
            Column("Node_r14c4t5n4", Integer),
            Column("Node_r14c4t6n1", Integer),
            Column("Node_r14c4t6n2", Integer),
            Column("Node_r14c4t6n3", Integer),
            Column("Node_r14c4t6n4", Integer),
            Column("Node_r14c4t7n1", Integer),
            Column("Node_r14c4t7n2", Integer),
            Column("Node_r14c4t7n3", Integer),
            Column("Node_r14c4t7n4", Integer),
            Column("Node_r14c4t8n1", Integer),
            Column("Node_r14c4t8n2", Integer),
            Column("Node_r14c4t8n3", Integer),
            Column("Node_r14c4t8n4", Integer),
        )

        # hpl_uc
        _: Table = Table(
            "hpl_uc",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("hsmp", Integer),
            Column("Time", DateTime),
            Column("Node_r14c3t1n1", Integer),
            Column("Node_r14c3t1n2", Integer),
            Column("Node_r14c3t1n3", Integer),
            Column("Node_r14c3t1n4", Integer),
            Column("Node_r14c3t2n1", Integer),
            Column("Node_r14c3t2n2", Integer),
            Column("Node_r14c3t2n3", Integer),
            Column("Node_r14c3t2n4", Integer),
            Column("Node_r14c3t3n1", Integer),
            Column("Node_r14c3t3n2", Integer),
            Column("Node_r14c3t3n3", Integer),
            Column("Node_r14c3t3n4", Integer),
            Column("Node_r14c3t4n1", Integer),
            Column("Node_r14c3t4n2", Integer),
            Column("Node_r14c3t4n3", Integer),
            Column("Node_r14c3t4n4", Integer),
            Column("Node_r14c3t5n1", Integer),
            Column("Node_r14c3t5n2", Integer),
            Column("Node_r14c3t5n3", Integer),
            Column("Node_r14c3t5n4", Integer),
            Column("Node_r14c3t6n1", Integer),
            Column("Node_r14c3t6n2", Integer),
            Column("Node_r14c3t6n3", Integer),
            Column("Node_r14c3t6n4", Integer),
            Column("Node_r14c3t7n1", Integer),
            Column("Node_r14c3t7n2", Integer),
            Column("Node_r14c3t7n3", Integer),
            Column("Node_r14c3t7n4", Integer),
            Column("Node_r14c3t8n1", Integer),
            Column("Node_r14c3t8n2", Integer),
            Column("Node_r14c3t8n3", Integer),
            Column("Node_r14c3t8n4", Integer),
            Column("Node_r14c4t1n1", Integer),
            Column("Node_r14c4t1n2", Integer),
            Column("Node_r14c4t1n3", Integer),
            Column("Node_r14c4t1n4", Integer),
            Column("Node_r14c4t2n1", Integer),
            Column("Node_r14c4t2n2", Integer),
            Column("Node_r14c4t2n3", Integer),
            Column("Node_r14c4t2n4", Integer),
            Column("Node_r14c4t3n1", Integer),
            Column("Node_r14c4t3n2", Integer),
            Column("Node_r14c4t3n3", Integer),
            Column("Node_r14c4t3n4", Integer),
            Column("Node_r14c4t4n1", Integer),
            Column("Node_r14c4t4n2", Integer),
            Column("Node_r14c4t4n3", Integer),
            Column("Node_r14c4t4n4", Integer),
            Column("Node_r14c4t5n1", Integer),
            Column("Node_r14c4t5n2", Integer),
            Column("Node_r14c4t5n3", Integer),
            Column("Node_r14c4t5n4", Integer),
            Column("Node_r14c4t6n1", Integer),
            Column("Node_r14c4t6n2", Integer),
            Column("Node_r14c4t6n3", Integer),
            Column("Node_r14c4t6n4", Integer),
            Column("Node_r14c4t7n1", Integer),
            Column("Node_r14c4t7n2", Integer),
            Column("Node_r14c4t7n3", Integer),
            Column("Node_r14c4t7n4", Integer),
            Column("Node_r14c4t8n1", Integer),
            Column("Node_r14c4t8n2", Integer),
            Column("Node_r14c4t8n3", Integer),
            Column("Node_r14c4t8n4", Integer),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)


class PostgreSQL(LAST):
    def __init__(self) -> None:
        super().__init__(
            uri="postgresql+psycopg2://admin:example@localhost:5432/research",
            username="admin",
            password="example",
        )


class MySQL(LAST):
    def __init__(self) -> None:
        super().__init__(
            uri="mysql+pymysql://root:example@localhost:3306/research",
            username="root",
            password="example",
        )


class MariaDB(LAST):
    def __init__(self) -> None:
        super().__init__(
            uri="mariadb+pymysql://root:example@localhost:3306/research",
            username="root",
            password="example",
        )


class DB2(LAST):
    def __init__(self) -> None:
        super().__init__(
            uri="db2+ibm_db://db2inst1:example@localhost:50000/research",
            username="db2inst1",
            password="example",
        )


class SQLite3(LAST):
    def __init__(self, fp: Path) -> None:
        self.fp: Path = fp.resolve()
        super().__init__(
            uri=f"sqlite:///{self.fp}",
            username="",
            password="",
        )


class InMemorySQLite3(LAST):
    def __init__(self) -> None:
        super().__init__(
            uri=f"sqlite:///:memory:",
            username="",
            password="",
        )


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


# class CouchDB(DocumentDB):
#     def __init__(self) -> None:
#         super().__init__(
#             url="http://localhost:5984",
#             username="root",
#             password="example",
#         )
#         self.db_url: str = self.url + "/" + self.database_name
#         self.headers: dict[str, str] = {"Content-Type": "application/json"}
#         self.auth: HTTPBasicAuth = HTTPBasicAuth(
#             username=self.username,
#             password=self.password,
#         )

#     def create_database(self) -> None:
#         if get(url=self.db_url, auth=self.auth).status_code == 200:
#             pass
#         else:
#             put(url=self.db_url, auth=self.auth)

#     def upload(self, data: str) -> Response:
#         return post(
#             url=self.db_url,
#             auth=self.auth,
#             headers=self.headers,
#             json=data,
#         )

#     def query_avg_value(self) -> None:
#         pass

#     def query_min_value(self) -> None:
#         pass

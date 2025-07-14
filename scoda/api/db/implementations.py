from scoda.api.db.llnl_last import LLNL_LAST
from scoda.api.db.theta import Theta
from pathlib import Path
from sqlalchemy import Table, Column, Float, Integer


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


class CouchDB:
    # TODO: implement this
    ...


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
            "benchmark_write_all_tables",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("seconds", Float),
        )

        _: Table = Table(
            "benchmark_per_table_write",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("cori_power_30_sec_seconds", Float),
            Column("hawk_power_15_min_seconds", Float),
            Column("lumi_power_10_min_seconds", Float),
            Column("marconi100_power_60_sec_seconds", Float),
            Column("perlmutter_power_60_sec_seconds", Float),
            Column("lumi_hpcg_seconds", Float),
            Column("hpcg_dpc_seconds", Float),
            Column("hpcg_spc_seconds", Float),
            Column("hpcg_uc_seconds", Float),
            Column("hpl_dpc_seconds", Float),
            Column("hpl_spc_seconds", Float),
            Column("hpl_uc_seconds", Float),
        )

        _: Table = Table(
            "benchmark_sequential_table_writes",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("cori_power_30_sec_seconds", Float),
            Column("hawk_power_15_min_seconds", Float),
            Column("lumi_power_10_min_seconds", Float),
            Column("marconi100_power_60_sec_seconds", Float),
            Column("perlmutter_power_60_sec_seconds", Float),
            Column("lumi_hpcg_seconds", Float),
            Column("hpcg_dpc_seconds", Float),
            Column("hpcg_spc_seconds", Float),
            Column("hpcg_uc_seconds", Float),
            Column("hpl_dpc_seconds", Float),
            Column("hpl_spc_seconds", Float),
            Column("hpl_uc_seconds", Float),
        )

        _: Table = Table(
            "benchmark_write_all_tables_row_by_row",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("seconds", Float),
        )

        _: Table = Table(
            "benchmark_write_per_tables_row_by_row",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("cori_power_30_sec_seconds", Float),
            Column("hawk_power_15_min_seconds", Float),
            Column("lumi_power_10_min_seconds", Float),
            Column("marconi100_power_60_sec_seconds", Float),
            Column("perlmutter_power_60_sec_seconds", Float),
            Column("lumi_hpcg_seconds", Float),
            Column("hpcg_dpc_seconds", Float),
            Column("hpcg_spc_seconds", Float),
            Column("hpcg_uc_seconds", Float),
            Column("hpl_dpc_seconds", Float),
            Column("hpl_spc_seconds", Float),
            Column("hpl_uc_seconds", Float),
        )
        self.metadata.create_all(bind=self.engine, checkfirst=True)

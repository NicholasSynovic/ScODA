from scoda.api.db.llnl_last import LLNL_LAST
from scoda.api.db import DocumentDB
from scoda.api.db.relational.theta import Theta
from pathlib import Path
from sqlalchemy import Table, Column, Float, Integer
from requests import get, put, post, Response
from requests.auth import HTTPBasicAuth
from typing import Any
import json


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

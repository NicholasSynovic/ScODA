"""
Utility functions.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import sys
from pathlib import Path
from time import time

import scoda.db.document.generic
import scoda.db.document.implementation
import scoda.db.relational.generic
import scoda.db.relational.implementation
import scoda.db.time_series.generic
import scoda.db.time_series.implementation


def resolve_path(filepath: str) -> Path:
    return Path(filepath).resolve()


def identify_input(key: str) -> str:
    split_key: list[str] = key.split(sep=".")

    return split_key[0]


def create_db_instance(
    db_name: str, last_dataset: bool = True
) -> (
    scoda.db.relational.generic.RelationalDB
    | scoda.db.document.generic.DocumentDB
    | scoda.db.time_series.generic.TimeSeriesDB
):
    if last_dataset:
        match db_name:
            # case "db2":
            #     return scoda_rdbms_engines.DB2()
            case "mariadb":
                return scoda.db.relational.implementation.MariaDB()
            case "mysql":
                return scoda.db.relational.implementation.MySQL()
            case "postgres":
                return scoda.db.relational.implementation.PostgreSQL()
            case "sqlite3":
                return scoda.db.relational.implementation.SQLite3(
                    fp=Path(f"{time()}.sqlite3"),
                )
            case "sqlite3-memory":
                return scoda.db.relational.implementation.InMemorySQLite3()
            case "couchdb":
                return scoda.db.document.implementation.CouchDB()
            case "mongodb":
                return scoda.db.document.implementation.MongoDB()
            case "influxdb":
                return scoda.db.time_series.implementation.InfluxDB()
            case "victoriametrics":
                return scoda.db.time_series.implementation.VictoriaMetrics()
            case _:
                sys.exit(100)

    else:
        pass
        # TODO: Implement these database classes
        # match db_name:
        #     case "db2":
        #         return scoda_rdbms_engines.DB2_Theta()
        #     case "mariadb":
        #         return scoda_rdbms_engines.MariaDB_Theta()
        #     case "mysql":
        #         return scoda_rdbms_engines.MySQL_Theta()
        #     case "postgres":
        #         return scoda_rdbms_engines.PostgreSQL_Theta()
        #     case "sqlite3":
        #         return scoda_rdbms_engines.SQLite3_Theta(
        #             fp=Path(f"{time()}_Theta.sqlite3")
        #         )
        #     case "sqlite3-memory":
        #         return scoda_rdbms_engines.InMemorySQLite3_Theta()

    return sys.exit(300)

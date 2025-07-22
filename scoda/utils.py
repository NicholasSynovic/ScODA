"""
Utility functions.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import sys
from pathlib import Path
from time import time

import scoda.db.relational as scoda_rdbms
import scoda.db.relational.engines as scoda_rdbms_engines


def resolve_path(filepath: str) -> Path:
    return Path(filepath).resolve()


def identify_input(key: str) -> str:
    split_key: list[str] = key.split(sep=".")

    return split_key[0]


def create_db_instance(db_name: str, last_dataset: bool = True) -> scoda_rdbms.RDBMS:
    if last_dataset:
        match db_name:
            case "db2":
                return scoda_rdbms_engines.DB2_LAST()
            case "mariadb":
                return scoda_rdbms_engines.MariaDB_LAST()
            case "mysql":
                return scoda_rdbms_engines.MySQL_LAST()
            case "postgres":
                return scoda_rdbms_engines.PostgreSQL_LAST()
            case "sqlite3":
                return scoda_rdbms_engines.SQLite3_LAST(
                    fp=Path(f"{time()}_last.sqlite3")
                )
            case "sqlite3-memory":
                return scoda_rdbms_engines.InMemorySQLite3_LAST()
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

"""
Utility functions.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import sys
from pathlib import Path
from time import time

import scoda.db.document.generic
import scoda.db.document.implementation
import scoda.db.lakehouse.generic
import scoda.db.lakehouse.implementation
import scoda.db.relational.generic
import scoda.db.relational.implementation
import scoda.db.time_series.generic
import scoda.db.time_series.implementation


def resolve_path(filepath: str) -> Path:
    """
    Resolve a given filepath to its absolute path.

    This function takes a filepath as input and returns an absolute Path object
    representing the resolved path.

    Arguments:
        filepath: The path to resolve.

    Returns:
        A Path object representing the resolved absolute path.

    """
    return Path(filepath).resolve()


def identify_input(key: str) -> str:
    """
    Extract the first element from a dotted string.

    This function takes a string where elements are separated by dots
    and returns the first element of the string.

    Arguments:
        key: A string where elements are separated by dots.

    Returns:
        The first element of the string.

    """
    split_key: list[str] = key.split(sep=".")

    return split_key[0]


def create_db_instance(  # noqa: PLR0911
    db_name: str,
) -> (
    scoda.db.relational.generic.RelationalDB
    | scoda.db.document.generic.DocumentDB
    | scoda.db.time_series.generic.TimeSeriesDB
    | scoda.db.lakehouse.generic.LakehouseDB
):
    """
    Create an instance of a database based on the provided database name.

    This function takes a database name as input and returns an appropriate
    database instance from the scoda library.  It supports various database
    types including relational (MariaDB, MySQL, PostgreSQL, SQLite), document
    (CouchDB, MongoDB), time-series (InfluxDB, VictoriaMetrics), and lakehouse
    (DeltaLake, Iceberg).

    Args:
        db_name: The name of the database to create an instance for.
                 Supported values are: "mariadb", "mysql", "postgres",
                 "sqlite3", "sqlite3-memory", "couchdb", "mongodb",
                 "influxdb", "victoriametrics", "deltalake", "iceberg".

    Returns:
        A database instance.

    """
    match db_name:
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
        case "deltalake":
            return scoda.db.lakehouse.implementation.DeltaLake()
        case "iceberg":
            return scoda.db.lakehouse.implementation.IcebergDB()
        case _:
            sys.exit(100)

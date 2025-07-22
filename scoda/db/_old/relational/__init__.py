"""
Relational database base class.

Copyright 2025 (C) Nicholas M. Synovic
"""

from scoda.db.relational.engines import (
    DB2_LAST,
    DB2_Theta,
    InMemorySQLite3_LAST,
    InMemorySQLite3_Theta,
    MariaDB_LAST,
    MariaDB_Theta,
    MySQL_LAST,
    MySQL_Theta,
    PostgreSQL_LAST,
    PostgreSQL_Theta,
    SQLite3_LAST,
    SQLite3_Theta,
)
from scoda.db.relational.schemas import RDBMS

__all__: list[str] = [
    "DB2_LAST",
    "DB2_Theta",
    "MariaDB_LAST",
    "MariaDB_Theta",
    "MySQL_LAST",
    "MySQL_Theta",
    "SQLite3_LAST",
    "SQLite3_Theta",
    "InMemorySQLite3_LAST",
    "InMemorySQLite3_Theta",
    "PostgreSQL_LAST",
    "PostgreSQL_Theta",
    "RDBMS",
]

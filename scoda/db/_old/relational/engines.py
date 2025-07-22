from pathlib import Path

from sqlalchemy import Column, DateTime, Integer, Table

import scoda.db.relational.schemas as scoda_relational_schemas


class DB2_LAST(scoda_relational_schemas.LASTSchema):
    def __init__(self) -> None:
        super().__init__(
            uri="db2+ibm_db://db2inst1:example@localhost:50000/research",
            username="db2inst1",
            password="example",
        )


class DB2_Theta(scoda_relational_schemas.ThetaSchema):
    def __init__(self) -> None:
        super().__init__(
            uri="db2+ibm_db://db2inst1:example@localhost:50000/research",
            username="db2inst1",
            password="example",
        )


class InMemorySQLite3_LAST(scoda_relational_schemas.LASTSchema):
    def __init__(self) -> None:
        super().__init__(
            uri=f"sqlite:///:memory:",
            username="",
            password="",
        )


class InMemorySQLite3_Theta(scoda_relational_schemas.ThetaSchema):
    def __init__(self) -> None:
        super().__init__(
            uri=f"sqlite:///:memory:",
            username="",
            password="",
        )


class MariaDB_LAST(scoda_relational_schemas.LASTSchema):
    def __init__(self) -> None:
        super().__init__(
            uri="mariadb+pymysql://root:example@localhost:3306/research",
            username="root",
            password="example",
        )


class MariaDB_Theta(scoda_relational_schemas.ThetaSchema):
    def __init__(self) -> None:
        super().__init__(
            uri="mariadb+pymysql://root:example@localhost:3306/research",
            username="root",
            password="example",
        )


class MySQL_LAST(scoda_relational_schemas.LASTSchema):
    def __init__(self) -> None:
        super().__init__(
            uri="mysql+pymysql://root:example@localhost:3307/research",
            username="root",
            password="example",
        )


class MySQL_Theta(scoda_relational_schemas.ThetaSchema):
    def __init__(self) -> None:
        super().__init__(
            uri="mysql+pymysql://root:example@localhost:3307/research",
            username="root",
            password="example",
        )


class PostgreSQL_LAST(scoda_relational_schemas.LASTSchema):
    def __init__(self) -> None:
        super().__init__(
            uri="postgresql+psycopg2://admin:example@localhost:5432/research",
            username="admin",
            password="example",
        )


class PostgreSQL_Theta(scoda_relational_schemas.ThetaSchema):
    def __init__(self) -> None:
        super().__init__(
            uri="postgresql+psycopg2://admin:example@localhost:5432/research",
            username="admin",
            password="example",
        )


class SQLite3_LAST(scoda_relational_schemas.LASTSchema):
    def __init__(self, fp: Path) -> None:
        self.fp: Path = fp.resolve()
        super().__init__(
            uri=f"sqlite:///{self.fp}",
            username="",
            password="",
        )


class SQLite3_Theta(scoda_relational_schemas.ThetaSchema):
    def __init__(self, fp: Path) -> None:
        self.fp: Path = fp.resolve()
        super().__init__(
            uri=f"sqlite:///{self.fp}",
            username="",
            password="",
        )

"""
Test to connect to a Postgres instance.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.dataframe import DataFrame

JDBC_POSTGRES_PATH: Path = Path("./jdbc/postgres.jar").resolve()
JDBC_POSTGRES_URI: str = "jdbc:postgresql://localhost:5432/research"
POSTGRES_USERNAME: str = "admin"
POSTGRES_PASSWORD: str = "example"  # noqa: S105


def main() -> None:
    """
    Initialize a Spark session and tests connectivity to a PostgreSQL database.

    This function configures the Spark session with the PostgreSQL JDBC driver,
    establishes a connection to a PostgreSQL database using provided credentials,
    reads the `contacts` table into a Spark DataFrame, and displays the contents.
    """
    # Initialize Spark session with PostgreSQL JDBC driver
    builder: SparkSession.Builder = SparkSession.builder
    builder = builder.appName("PostgresTestConnection")
    builder = builder.config(key="spark.jars", value=str(JDBC_POSTGRES_PATH))

    session: SparkSession = builder.getOrCreate()

    properties: dict[str, str] = {
        "user": POSTGRES_USERNAME,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    # Read a table into a Spark DataFrame
    df: DataFrame = session.read.jdbc(
        url=JDBC_POSTGRES_URI,
        table="contacts",
        properties=properties,
    )

    # Show the data
    df.show()


if __name__ == "__main__":
    main()

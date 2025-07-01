"""
Test to connect to a MySQL instance.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.dataframe import DataFrame

JDBC_MYSQL_PATH: Path = Path("./jdbc/mysql.jar").resolve()
JDBC_MYSQL_URI: str = "jdbc:mysql://localhost:3306/research"
MYSQL_USERNAME: str = "root"
MYSQL_PASSWORD: str = "example"  # noqa: S105


def main() -> None:
    """
    Initialize a Spark session and tests connectivity to a MySQL database.

    This function configures the Spark session with the MySQL JDBC driver,
    establishes a connection to a MySQL database using provided credentials,
    reads the `contacts` table into a Spark DataFrame, and displays the contents.
    """
    # Initialize Spark session with MySQL JDBC driver
    builder: SparkSession.Builder = SparkSession.builder
    builder = builder.appName("MySQLTestConnection")
    builder = builder.config(key="spark.jars", value=str(JDBC_MYSQL_PATH))

    session: SparkSession = builder.getOrCreate()

    properties: dict[str, str] = {
        "user": MYSQL_USERNAME,
        "password": MYSQL_PASSWORD,
        "driver": "com.mysql.cj.jdbc.Driver",
    }

    # Read a table into a Spark DataFrame
    df: DataFrame = session.read.jdbc(
        url=JDBC_MYSQL_URI,
        table="contacts",
        properties=properties,
    )

    # Show the data
    df.show()


if __name__ == "__main__":
    main()

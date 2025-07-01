"""
Test to connect to a IBM DB2 instance.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.dataframe import DataFrame

JDBC_DB2_PATH: Path = Path("./jdbc/db2.jar").resolve()
JDBC_DB2_URI: str = "jdbc:db2://localhost:50000/research"
DB2_USERNAME: str = "db2inst1"
DB2_PASSWORD: str = "example"  # noqa: S105


def main() -> None:
    """
    Initialize a Spark session and tests connectivity to a IBM DB2 database.

    This function configures the Spark session with the IBM DB2 JDBC driver,
    establishes a connection to a IBM DB2 database using provided credentials,
    reads the `contacts` table into a Spark DataFrame, and displays the contents.
    """
    # Initialize Spark session with IBM DB2 JDBC driver
    builder: SparkSession.Builder = SparkSession.builder
    builder = builder.appName("IBMDB2TestConnection")
    builder = builder.config(key="spark.jars", value=str(JDBC_DB2_PATH))

    session: SparkSession = builder.getOrCreate()

    properties: dict[str, str] = {
        "user": DB2_USERNAME,
        "password": DB2_PASSWORD,
        "driver": "com.ibm.db2.jcc.DB2Driver",
        # "dbtable": "research"
    }

    # Read a table into a Spark DataFrame
    df: DataFrame = session.read.jdbc(
        url=JDBC_DB2_URI,
        table="research",
        properties=properties,
    )

    # Show the data
    df.show()


if __name__ == "__main__":
    main()

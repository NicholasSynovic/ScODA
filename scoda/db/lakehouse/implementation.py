"""
Implementations of lakehouse.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from pathlib import Path

import delta
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

from scoda.datasets.generic import Dataset
from scoda.db.lakehouse.generic import LakehouseDB


class DeltaLake(LakehouseDB):
    """
    DeltaLake database interface using Apache Spark with Delta Lake integration.

    This class provides an interface for reading, writing, and managing data stored
    in the Delta Lake format, which supports ACID transactions and scalable metadata
    handling on data lakes. It initializes a SparkSession with the appropriate
    configurations to enable Delta Lake functionality.

    Attributes:
        base_path (Path): Filesystem path where Delta tables are stored.
        spark (SparkSession): Configured Spark session with Delta Lake support.

    """

    def __init__(
        self,
        base_path: str = "deltalake",
        convert_time_column_to_int: bool = False,  # noqa: FBT001, FBT002
    ) -> None:
        """
        Initialize a DeltaLake database interface.

        Sets the base path for storing Delta tables and initializes a Spark
        session with Delta Lake extensions and catalog support. Designed for
        Lakehouse-style data operations with the Delta Lake format.

        Args:
            base_path: Directory path where Delta tables will be stored or
                accessed.
            convert_time_column_to_int: If True, convert time columns to integer
                representations for compatibility and performance.

        """
        super().__init__(convert_time_column_to_int=convert_time_column_to_int)
        self.base_path = Path(base_path)
        builder = (
            pyspark.sql.SparkSession.builder.appName("MyApp")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        self.spark: SparkSession = delta.configure_spark_with_delta_pip(
            builder
        ).getOrCreate()

    def _table_path(self, table_name: str) -> str:
        return str(self.base_path / table_name)

    def create(self) -> None:
        """Create the database if it does not already exist."""
        self.base_path.mkdir(parents=True, exist_ok=True)

    def delete(self) -> None:
        """
        Delete all data from the database.

        This method sends removes all stored data.

        """
        pass  # noqa: PIE790

    def batch_upload(self, dataset: Dataset) -> None:
        """
        Upload data in batch to the database.

        This method iterates over a dataset and sends each record to the
        database.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` instance.

        """
        df = self.spark.createDataFrame(dataset.data)
        df.write.format("delta").mode("overwrite").save(self._table_path(dataset.name))

    def batch_read(self, table_name: str) -> None:
        """
        Perform a batch read of all metrics from the database.

        Arguments:
            table_name: The name of the table to read from.

        """
        self.spark.read.format("delta").load(self._table_path(table_name))

    def sequential_upload(self, dataset: Dataset) -> None:
        """
        Upload data sequentially to the database.

        This method iterates over each row in a dataset and writes it
        individually to the database with a short delay between uploads. Each
        row is converted to a line protocol point with appropriate tags and
        fields. Invalid or missing field values are skipped, and non-numeric
        strings are cast to float where possible.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` containing time series
                data with a time index and one or more value columns.

        """
        df = self.spark.createDataFrame(dataset.data)
        df.write.format("delta").mode("append").save(self._table_path(dataset.name))

    def sequential_read(self, table_name: str, rows: int) -> None:
        """
        Perform a sequential read of records from a specified table.

        This method constructs and executes a  query to read all records from
        the specified table. It counts the number of records streamed from the
        query result but does not return or store them.

        Arguments:
            table_name: The name of the table (i.e., measurement) to read from.
            rows: The number of rows to read.

        """
        rows += 1
        self.spark.read.format("delta").load(self._table_path(table_name))

    def query_average_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query the average value of a column in a given table.

        This method retrieves the average value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the average value.

        """
        df = self.spark.read.format("delta").load(self._table_path(table_name))
        df.select(F.avg(F.col(column_name)).alias("avg")).collect()[0]["avg"]

    def query_max_value(self, table_name: str, column_name: str) -> None:
        """
        Query the maximum value of a column in a given table.

        This method retrieves the maximum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the maximum value.

        """
        df = self.spark.read.format("delta").load(self._table_path(table_name))
        df.select(F.max(F.col(column_name)).alias("max")).collect()[0]["max"]

    def query_min_value(self, table_name: str, column_name: str) -> None:
        """
        Query the minimum value of a column in a given table.

        This method retrieves the minimum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the minimum value.

        """
        df = self.spark.read.format("delta").load(self._table_path(table_name))
        df.select(F.min(F.col(column_name)).alias("min")).collect()[0]["min"]

    def query_mode_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query the mode value from a specified table and column.

        It is intended to support analysis for determining the most frequent
        (mode) value within the specified table and column.

        Arguments:
            table_name: The name of the table to query.
            column_name: The name of the column to extract the mode value from.

        """
        df = self.spark.read.format("delta").load(self._table_path(table_name))
        df.groupBy(column_name).count().orderBy(F.desc("count")).limit(1).collect()[0][
            column_name
        ]

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query a time window value grouped by a table name and column name.

        This function sends a request to group a specified column over an one
        hour time window.

        Arguments:
            table_name: The name of the table to group by.
            column_name: The name of the column to average over time.

        """
        df = self.spark.read.format("delta").load(self._table_path(table_name))

        df = df.withColumn(column_name, F.col(column_name).cast("timestamp"))
        df.groupBy(F.window(column_name, "1 hour")).agg(
            F.avg(F.col(column_name)).alias("average_value")
        ).orderBy("window")


class IcebergDB(LakehouseDB):
    """
    Interface for interacting with an Apache Iceberg data lake using PySpark.

    This class sets up a Spark session configured for Iceberg with a local or
    remote warehouse directory. It allows reading and writing structured
    datasets in a tabular format compatible with Iceberg.

    Attributes:
        warehouse_path: Path to the Iceberg warehouse (local or cloud URI).
        catalog_name: Name of the Spark catalog used for Iceberg.
        spark: Configured SparkSession with Iceberg integration.

    Args:
        warehouse_path: Path to the Iceberg warehouse (default is "iceberg").
        catalog_name: Name of the Iceberg catalog (default is "hadoop_prod").
        convert_time_column_to_int: Whether to convert time columns to integers
            when uploading data.

    """

    def __init__(
        self,
        warehouse_path: str = "iceberg",
        catalog_name: str = "hadoop_prod",
        convert_time_column_to_int: bool = False,  # noqa: FBT001, FBT002
    ) -> None:
        """
        Initialize the IcebergDB.

        Sets up a local or remote warehouse path and configures the Spark
        catalog for reading and writing Iceberg tables. Designed to support
        Lakehouse-style data operations using the Iceberg table format.

        Args:
            warehouse_path: Path to the Iceberg warehouse directory. Can be a
                local file path or a cloud URI (e.g., "s3a://bucket/warehouse").
            catalog_name: The name of the Spark catalog used to register Iceberg
                tables.
            convert_time_column_to_int: Whether to convert time columns to
                integer representations when storing datasets.

        """
        super().__init__(convert_time_column_to_int=convert_time_column_to_int)
        self.warehouse_path = Path(warehouse_path).resolve()
        self.catalog_name = catalog_name

        self.spark = (
            SparkSession.builder.appName("IcebergExample")
            .config(
                "spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
            .config(
                "spark.sql.catalog.hadoop_prod.warehouse", f"file:{self.warehouse_path}"
            )  # or s3a://bucket/...
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .getOrCreate()
        )

    def _qualified_table(self, table_name: str) -> str:
        """
        Get the fully qualified table name.

        Returns:
            str = Name of the qualified table

        """
        return f"{self.catalog_name}.{table_name}"

    def create(self) -> None:
        """Create the database if it does not already exist."""
        Path(self.warehouse_path).mkdir(parents=True, exist_ok=True)
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS hadoop_prod.db1")

    def delete(self) -> None:
        """
        Delete all data from the database.

        This method sends removes all stored data.

        """
        self.spark.sql("DROP SCHEMA IF EXISTS hadoop_prod.db1")

    def batch_upload(self, dataset: Dataset) -> None:
        """
        Upload data in batch to the database.

        This method iterates over a dataset and sends each record to the
        database.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` instance.

        """
        df = self.spark.createDataFrame(dataset.data)
        df.writeTo(self._qualified_table(dataset.name)).using(
            "iceberg"
        ).createOrReplace()

    def sequential_upload(self, dataset: Dataset) -> None:
        """
        Upload data sequentially to the database.

        This method iterates over each row in a dataset and writes it
        individually to the database with a short delay between uploads. Each
        row is converted to a line protocol point with appropriate tags and
        fields. Invalid or missing field values are skipped, and non-numeric
        strings are cast to float where possible.

        Arguments:
            dataset: A `scoda.datasets.generic.Dataset` containing time series
                data with a time index and one or more value columns.

        """
        df = self.spark.createDataFrame(dataset.data)
        df.writeTo(self._qualified_table(dataset.name)).append()

    def batch_read(self, table_name: str) -> None:
        """
        Perform a batch read of all metrics from the database.

        Arguments:
            table_name: The name of the table to read from.

        """
        self.spark.read.table(self._qualified_table(table_name))

    def sequential_read(self, table_name: str, rows: int) -> None:
        """
        Perform a sequential read of records from a specified table.

        This method constructs and executes a  query to read all records from
        the specified table. It counts the number of records streamed from the
        query result but does not return or store them.

        Arguments:
            table_name: The name of the table (i.e., measurement) to read from.
            rows: The number of rows to read.

        """
        return self.spark.read.table(self._qualified_table(table_name)).limit(rows)

    def query_average_value(self, table_name: str, column_name: str) -> None:
        """
        Query the average value of a column in a given table.

        This method retrieves the average value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the average value.

        """
        df = self.spark.read.table(self._qualified_table(table_name))
        df.select(F.avg(F.col(column_name))).collect()[0][0]

    def query_max_value(self, table_name: str, column_name: str) -> None:
        """
        Query the maximum value of a column in a given table.

        This method retrieves the maximum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the maximum value.

        """
        df = self.spark.read.table(self._qualified_table(table_name))
        df.select(F.max(F.col(column_name))).collect()[0][0]

    def query_min_value(self, table_name: str, column_name: str) -> None:
        """
        Query the minimum value of a column in a given table.

        This method retrieves the minimum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the minimum value.

        """
        df = self.spark.read.table(self._qualified_table(table_name))
        df.select(F.min(F.col(column_name))).collect()[0][0]

    def query_mode_value(self, table_name: str, column_name: str) -> None:
        """
        Query the mode value from a specified table and column.

        It is intended to support analysis for determining the most frequent
        (mode) value within the specified table and column.

        Arguments:
            table_name: The name of the table to query.
            column_name: The name of the column to extract the mode value from.

        """
        df = self.spark.read.table(self._qualified_table(table_name))
        df.groupBy(column_name).count().orderBy(F.desc("count")).limit(1).collect()[0][
            0
        ]

    def query_groupby_time_window_value(
        self,
        table_name: str,
        column_name: str,
    ) -> None:
        """
        Query a time window value grouped by a table name and column name.

        This function sends a request to group a specified column over an one
        hour time window.

        Arguments:
            table_name: The name of the table to group by.
            column_name: The name of the column to average over time.

        """
        df = self.spark.read.table(self._qualified_table(table_name))
        df = df.withColumn(column_name, F.col(column_name).cast("timestamp"))
        df.groupBy(F.window(column_name, "1 hour")).agg(
            F.avg(F.col(column_name)).alias("average_value")
        ).orderBy("window")

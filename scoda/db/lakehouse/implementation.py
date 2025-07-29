from pathlib import Path

import delta
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from scoda.datasets.generic import Dataset
from scoda.db.lakehouse.generic import LakehouseDB


class DeltaLake(LakehouseDB):
    def __init__(
        self,
        base_path: str = "deltalake",
        convert_time_column_to_int: bool = False,
    ) -> None:
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
        self.base_path.mkdir(parents=True, exist_ok=True)

    def delete(self) -> None:
        """
        Delete all data from the database.

        This method sends removes all stored data.

        """
        pass

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

    def batch_read(self, table_name: str):
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
            dataset: A `scoda.datasets.generic.Dataset` containing time series data
                    with a time index and one or more value columns.

        """
        df = self.spark.createDataFrame(dataset.data)
        df.write.format("delta").mode("append").save(self._table_path(dataset.name))

    def sequential_read(self, table_name: str, rows: int):
        """
        Perform a sequential read of records from a specified table.

        This method constructs and executes a  query to read all records from
        the specified table. It counts the number of records streamed from the
        query result but does not return or store them.

        Arguments:
            table_name: The name of the table (i.e., measurement) to read from.
            rows: The number of rows to read.

        """
        self.spark.read.format("delta").load(self._table_path(table_name))

    def query_average_value(self, table_name: str, column_name: str):
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

    def query_max_value(self, table_name: str, column_name: str):
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

    def query_min_value(self, table_name: str, column_name: str):
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

    def query_mode_value(self, table_name: str, column_name: str):
        """
        Query the mode value from a specified table and column.

        It is intended to support analysis for determining the most frequent
        (mode) value within the specified table and column.

        Arguments:
            table_name: The name of the table to query.
            column_name: The name of the column to extract the mode value from.

        """
        df = self.spark.read.format("delta").load(self._table_path(table_name))
        mode_val = (
            df.groupBy(column_name)
            .count()
            .orderBy(F.desc("count"))
            .limit(1)
            .collect()[0][column_name]
        )

    def query_groupby_time_window_value(self, table_name: str, column_name: str):
        """
        Query a time window value grouped by a table name and column name.

        This function sends a request to group a specified column over an one
        hour time window.

        Arguments:
            table_name: The name of the table to group by.
            column_name: The name of the column to average over time.

        """
        df = self.spark.read.format("delta").load(self._table_path(table_name))
        if column_name not in df.columns:
            raise ValueError("Expected 'time' column in table for time-based grouping.")

        df = df.withColumn(column_name, F.col(column_name).cast("timestamp"))
        grouped = (
            df.groupBy(F.window(column_name, "1 hour"))
            .agg(F.avg(F.col(column_name)).alias("average_value"))
            .orderBy("window")
        )


class IcebergDB(LakehouseDB):
    def __init__(
        self,
        warehouse_path: str = "iceberg",
        catalog_name: str = "hadoop_prod",
        convert_time_column_to_int: bool = False,
    ) -> None:
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
        """Helper method to get the fully qualified table name."""
        return f"{self.catalog_name}.{table_name}"

    def create(self) -> None:
        """Create the warehouse directory if it doesn't exist."""
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
            dataset: A `scoda.datasets.generic.Dataset` containing time series data
                    with a time index and one or more value columns.

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

    def sequential_read(self, table_name: str, rows: int):
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

    def query_max_value(self, table_name: str, column_name: str):
        """
        Query the maximum value of a column in a given table.

        This method retrieves the maximum value of a specified column from a
        time series table using the configured database endpoint.

        Arguments:
            table_name: The name of the table.
            column_name: The column from which to retrieve the maximum value.

        """
        df = self.spark.read.table(self._qualified_table(table_name))
        max_val = df.select(F.max(F.col(column_name))).collect()[0][0]
        return max_val

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

    def query_mode_value(self, table_name: str, column_name: str):
        """
        Query the mode value from a specified table and column.

        It is intended to support analysis for determining the most frequent
        (mode) value within the specified table and column.

        Arguments:
            table_name: The name of the table to query.
            column_name: The name of the column to extract the mode value from.

        """
        df = self.spark.read.table(self._qualified_table(table_name))
        mode_val = (
            df.groupBy(column_name)
            .count()
            .orderBy(F.desc("count"))
            .limit(1)
            .collect()[0][0]
        )
        return mode_val

    def query_groupby_time_window_value(self, table_name: str, column_name: str):
        """
        Query a time window value grouped by a table name and column name.

        This function sends a request to group a specified column over an one
        hour time window.

        Arguments:
            table_name: The name of the table to group by.
            column_name: The name of the column to average over time.

        """

        df = self.spark.read.table(self._qualified_table(table_name))
        if column_name not in df.columns:
            raise ValueError("Expected 'time' column for time window operations")

        df = df.withColumn(column_name, F.col(column_name).cast("timestamp"))
        grouped = (
            df.groupBy(F.window(column_name, "1 hour"))
            .agg(F.avg(F.col(column_name)).alias("average_value"))
            .orderBy("window")
        )
        return grouped

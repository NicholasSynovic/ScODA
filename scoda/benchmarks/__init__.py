"""
Top level benchmarks/__init__.py file.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from collections.abc import Iterable

import scoda.benchmarks.aggregation as scoda_benchmarks_aggregation
import scoda.benchmarks.egress as scoda_benchmarks_egress
import scoda.benchmarks.ingress as scoda_benchmarks_ingress
import scoda.datasets.generic
import scoda.db


def run_benchmarks(
    test_db: scoda.db.DB,
    results_db: scoda.db.Results,
    datasets: Iterable[scoda.datasets.generic.Dataset],
    iterations: int,
) -> None:
    """
    Run a full suite of ingress, egress, and aggregation tests on a database.

    This function evaluates database performance by writing and reading multiple
    datasets using different ingestion and egress strategies, as well as
    executing aggregation queries. Benchmark results are recorded in the
    specified results database for further analysis.

    If the test database is configured to convert time columns to integers, all
    datasets will be updated accordingly. Existing data in the test database is
    cleared before benchmarking.

    Arguments:
        test_db: The database instance to run benchmarks on.
        results_db: The database used to store benchmark results.
        datasets: An iterable of datasets used for benchmarking.
        iterations: The number of times to repeat each benchmark operation.

    """
    # Change the type of the time column
    if test_db.convert_time_column_to_int:
        ds: scoda.datasets.generic.Dataset
        for ds in datasets:
            ds.data[ds.time_column] = ds.data[ds.time_column].apply(
                lambda x: int(x.timestamp())
            )

    # Clear existing data
    test_db.recreate()

    # Ingress benchmarks
    scoda_benchmarks_ingress.batch_write_all_tables(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    scoda_benchmarks_ingress.batch_write_individual_tables(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    scoda_benchmarks_ingress.sequential_write_all_tables(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    scoda_benchmarks_ingress.sequential_write_individual_tables(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    # Garuentee that data is present
    for dataset in datasets:
        test_db.batch_upload(dataset=dataset)

    # Egress benchmarks
    scoda_benchmarks_egress.batch_read_all_tables(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    scoda_benchmarks_egress.batch_read_individual_tables(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    scoda_benchmarks_egress.sequential_read_all_tables(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    scoda_benchmarks_egress.sequential_read_individual_tables(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    # Aggregation benchmarks
    scoda_benchmarks_aggregation.query_average_value(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    scoda_benchmarks_aggregation.query_groupby_time_window(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    scoda_benchmarks_aggregation.query_max_value(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    scoda_benchmarks_aggregation.query_min_value(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

    scoda_benchmarks_aggregation.query_mode_value(
        test_db=test_db,
        iterations=iterations,
        results_db=results_db,
        datasets=datasets,
    )

"""
Database benchmarks.

Copyright 2025 (C) Nicholas M. Synovic

"""

from collections.abc import Iterable

import scoda.benchmarks.aggregation as scoda_benchmarks_aggregation
import scoda.benchmarks.egress as scoda_benchmarks_egress
import scoda.benchmarks.ingress as scoda_benchmarks_ingress
import scoda.datasets as scoda_datasets
import scoda.db as scoda_db
import scoda.db.results as scoda_db_results

__all__: list[str] = ["run_benchmarks"]


def run_benchmarks(
    test_db: scoda_db.DB,
    results_db: scoda_db_results.Results,
    datasets: Iterable[scoda_datasets.Dataset],
    iterations: int,
) -> None:
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

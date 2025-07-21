"""
Database benchmarks.

Copyright 2025 (C) Nicholas M. Synovic

"""

from scoda.benchmarks.aggregation import (
    query_average_value,
    query_groupby_time_window,
    query_max_value,
    query_median_value,
    query_min_value,
)
from scoda.benchmarks.egress import (
    batch_read_all_tables,
    batch_read_individual_tables,
    sequential_read_all_tables,
    sequential_read_individual_tables,
)
from scoda.benchmarks.ingress import (
    batch_write_all_tables,
    batch_write_individual_tables,
    sequential_write_all_tables,
    sequential_write_individual_tables,
)

__all__: list[str] = [
    "batch_read_all_tables",
    "batch_read_individual_tables",
    "batch_write_all_tables",
    "batch_write_individual_tables",
    "query_average_value",
    "query_groupby_time_window",
    "query_max_value",
    "query_median_value",
    "query_min_value",
    "sequential_read_all_tables",
    "sequential_read_individual_tables",
    "sequential_write_all_tables",
    "sequential_write_individual_tables",
]

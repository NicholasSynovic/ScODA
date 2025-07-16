#!/bin/bash

# SQLite3 Memory
scoda last \
    --input-dir benchmark_datasets/LAST_TINY/Power-Provisioning-Dataset \
    --output benchmark_results/sqlite3-memory_last-tiny_10.sqlite3 \
    --iterations 10 \
    --db sqlite3-memory

# SQLite3
scoda last \
    --input-dir benchmark_datasets/LAST_TINY/Power-Provisioning-Dataset \
    --output benchmark_results/sqlite3_last-tiny_10.sqlite3 \
    --iterations 10 \
    --db sqlite3
rm *_last.sqlite3

# MariaDB
scoda last \
    --input-dir benchmark_datasets/LAST_TINY/Power-Provisioning-Dataset \
    --output benchmark_results/mariadb_last-tiny_10.sqlite3 \
    --iterations 10 \
    --db mariadb

#MySQL
scoda last \
    --input-dir benchmark_datasets/LAST_TINY/Power-Provisioning-Dataset \
    --output benchmark_results/mysql_last-tiny_10.sqlite3 \
    --iterations 10 \
    --db mysql

# PostgreSQL
scoda last \
    --input-dir benchmark_datasets/LAST_TINY/Power-Provisioning-Dataset \
    --output benchmark_results/postgresql_last-tiny_10.sqlite3 \
    --iterations 10 \
    --db postgres

#!/bin/bash

DATASET_DIR="./benchmark_datasets/LAST_TINY/Power-Provisioning-Dataset"
BENCHMARK_RESULTS_DIR="./benchmark_results"
ITERATIONS=100

# SQLite3 Memory
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/sqlite3-memory_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db sqlite3-memory

# SQLite3
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/sqlite3_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db sqlite3
rm *_last.sqlite3

# MariaDB
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/mariadb_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db mariadb

#MySQL
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/mysql_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db mysql

# PostgreSQL
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/postgresql_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db postgres

# CouchDB
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/couchdb_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db couchdb

# MongoDB
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/mongodb_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db mongodb

# InfluxDB
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/influxdb_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db influxdb

# VictoriaMetrics
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/victoriametrics_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db victoriametrics

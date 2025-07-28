#!/bin/bash

DATASET_DIR="./benchmark_datasets/LAST_TINY/Power-Provisioning-Dataset"
BENCHMARK_RESULTS_DIR="./benchmark_results"
ITERATIONS=1

# SQLite3 Memory
echo "===SQLite3 Memory==="
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/sqlite3-memory_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db sqlite3-memory

# SQLite3
echo "===SQLite3==="
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/sqlite3_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db sqlite3

# PostgreSQL
echo "===PostgreSQL==="
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/postgresql_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db postgres

# MySQL
echo "===MySQL==="
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/mysql_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db mysql

# MariaDB
echo "===MariaDB==="
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/mariadb_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db mariadb

# CouchDB
echo "===CouchDB==="
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/couchdb_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db couchdb

# MongoDB
echo "===MongoDB==="
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/mongodb_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db mongodb

# InfluxDB
echo "===InfluxDB==="
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/influxdb_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db influxdb

VictoriaMetrics
echo "===VictoriaMetrics==="
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/victoriametrics_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db victoriametrics

Delta Lake
echo "===Delta Lake==="
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/deltalake_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db deltalake

# Apache Iceberg
echo "===Apache Iceberg==="
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/iceberg_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db iceberg

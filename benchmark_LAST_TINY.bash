#!/bin/bash

DATASET_DIR="./benchmark_datasets/LAST_TINY/Power-Provisioning-Dataset"
BENCHMARK_RESULTS_DIR="./benchmark_results"
ITERATIONS=10

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
export POSTGRESQL_USERNAME="admin"
export POSTGRESQL_PASSWORD="example"
export POSTGRESQL_URI="localhost:5432"
export POSTGRESQL_DATABASE="research"
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/postgresql_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db postgres

# MySQL
echo "===MySQL==="
export MYSQL_USERNAME="root"
export MYSQL_PASSWORD="example"
export MYSQL_URI="localhost:3307"
export MYSQL_DATABASE="research"
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/mysql_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db mysql

# MariaDB
echo "===MariaDB==="
export MARIADB_USERNAME="root"
export MARIADB_PASSWORD="example"
export MARIADB_URI="localhost:3306"
export MARIADB_DATABASE="research"
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/mariadb_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db mariadb

# CouchDB
echo "===CouchDB==="
export COUCHDB_USERNAME="root"
export COUCHDB_PASSWORD="example"
export COUCHDB_URI="localhost:5984"
export COUCHDB_DATABASE="research"
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/couchdb_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db couchdb

# MongoDB
echo "===MongoDB==="
export MONOGDB_USERNAME="root"
export MONOGDB_PASSWORD="example"
export MONGODB_URI="localhost:27017"
export MONGODB_DATABASE="research"
export MONGODB_COLLECTION="research_data"
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/mongodb_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db mongodb

# InfluxDB
echo "===InfluxDB==="
export INFLUXDB_TOKEN="lsJ1cR9FBJojeZ-HGzN9kAdhc1XYcagfq8MkaKAmeMJP_Ux2hOOu3n-84aSNP0EaP_kIXGdaByl19MP6938WuA=="
export INFLUXDB_BUCKET="research"
export INFLUXDB_ORG="research"
export INFLUXDB_URI="http://localhost:8086"
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/influxdb_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db influxdb

# VictoriaMetrics
echo "===VictoriaMetrics==="
export VICTORIAMETRICS_URI="http://localhost:8428"
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/victoriametrics_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db victoriametrics

# Delta Lake
echo "===Delta Lake==="
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/deltalake_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db deltalake

# Apache Iceberg
echo "===Apache Iceberg==="
export PYSPARK_SUBMIT_ARGS="--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 pyspark-shell"
scoda last \
    --input-dir $DATASET_DIR \
    --output $BENCHMARK_RESULTS_DIR/iceberg_last-tiny_$ITERATIONS.sqlite3 \
    --iterations $ITERATIONS \
    --db iceberg

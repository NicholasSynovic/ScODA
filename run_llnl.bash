#!/bin/bash

# SQLite3 file
python scoda/main.py \
    -d sqlite3-llnl \
    -i LAST/Power-Provisioning-Dataset \
    -o sqlite3-llnl_10.sqlite3 \
    -r 10

# SQLite3 in-memory
python scoda/main.py \
    -d sqlite3-memory-llnl \
    -i LAST/Power-Provisioning-Dataset \
    -o sqlite3-memory-llnl_10.sqlite3 \
    -r 10

# PostgreSQL
docker compose -f docker/postgresql.docker-compose.yml up -d
python scoda/main.py \
    -d postgres-llnl \
    -i LAST/Power-Provisioning-Dataset \
    -o postgres-llnl_10.sqlite3 \
    -r 10
docker compose -f docker/postgresql.docker-compose.yml down

# MySQL
docker compose -f docker/mysql.docker-compose.yml up -d
python scoda/main.py \
    -d mysql-llnl \
    -i LAST/Power-Provisioning-Dataset \
    -o mysql-llnl_10.sqlite3 \
    -r 10
docker compose -f docker/mysql.docker-compose.yml down

# MariaDB
docker compose -f docker/mariadb.docker-compose.yml up -d
python scoda/main.py \
    -d mariadbmysql-llnl \
    -i LAST/Power-Provisioning-Dataset \
    -o mariadb-llnl_10.sqlite3 \
    -r 10
docker compose -f docker/mariadb.docker-compose.yml down

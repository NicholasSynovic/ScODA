#!/bin/bash

python scoda/main.py \
    -d sqlite3-llnl \
    -i LAST/Power-Provisioning-Dataset \
    -o sqlite3-llnl_10.sqlite3 \
    -r 10

python scoda/main.py \
    -d sqlite3-memory-llnl \
    -i LAST/Power-Provisioning-Dataset \
    -o sqlite3-memory-llnl_10.sqlite3 \
    -r 10

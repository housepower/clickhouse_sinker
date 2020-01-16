#!/usr/bin/env bash

set -e

clickhouse client -n <<-EOSQL
    CREATE DATABASE IF NOT EXISTS docker;
    USE docker;
    CREATE TABLE IF NOT EXISTS docker.docker (x Int32) ENGINE = Log;
EOSQL

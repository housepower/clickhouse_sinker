#!/usr/bin/env bash

set -e

clickhouse client -n <<-EOSQL
    USE default;
    CREATE TABLE IF NOT EXISTS logstash (
        date Date,
        level String,
        message String
    ) ENGINE = SummingMergeTree(date, (date, level, message), 8192);


EOSQL

#  CREATE TABLE IF NOT EXISTS logstash (
#    date Date,
#    level String,
#    message String
#  ) ENGINE = SummingMergeTree();
#

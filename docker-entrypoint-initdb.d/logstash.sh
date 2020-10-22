#!/usr/bin/env bash

set -e

clickhouse client -n <<-EOSQL
    USE default;
    CREATE TABLE IF NOT EXISTS logstash (
      date Date,
      date_nullable Nullable(Date),
      level String,
      message String,
      str String,
      str_nullable Nullable(String),
      num Int64,
      num_nullable Nullable(Int64),
      fnum Float64,
      fnum_nullable Nullable(Float64)
    )
    ENGINE = SummingMergeTree(date, (date, level, message), 8192);


EOSQL

#  CREATE TABLE IF NOT EXISTS logstash (
#    date Date,
#    level String,
#    message String
#  ) ENGINE = SummingMergeTree();
#

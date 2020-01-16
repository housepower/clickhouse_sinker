#!/usr/bin/env bash

set -e

clickhouse client -n <<-EOSQL
  USE default;

  CREATE TABLE IF NOT EXISTS daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);
EOSQL

#!/usr/bin/env bash

echo "create database"
curl "localhost:8123" -d "CREATE DATABASE IF NOT EXISTS gauge ON CLUSTER abc"

echo "create metric test tables"
curl "localhost:8123" -d "DROP TABLE IF EXISTS test_prom_metric ON CLUSTER abc SYNC"
curl "localhost:8123" -d "CREATE TABLE test_prom_metric ON CLUSTER abc
(
    __series_id Int64,
    timestamp DateTime CODEC(DoubleDelta, LZ4),
    value Float32 CODEC(ZSTD(15))
) ENGINE=ReplicatedReplacingMergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (__series_id, timestamp);"

curl "localhost:8123" -d "DROP TABLE IF EXISTS dist_test_prom_metric ON CLUSTER abc SYNC"
curl "localhost:8123" -d "CREATE TABLE dist_test_prom_metric ON CLUSTER abc AS test_prom_metric ENGINE = Distributed(abc, default, test_prom_metric);"

curl "localhost:8123" -d "DROP TABLE IF EXISTS test_prom_series ON CLUSTER abc SYNC"
curl "localhost:8123" -d "CREATE TABLE test_prom_series ON CLUSTER abc
(
    __series_id Int64,
    __mgmt_id Int64,
    labels String,
    __name__ String
) ENGINE=ReplicatedReplacingMergeTree()
ORDER BY (__name__, __series_id);"

curl "localhost:8123" -d "DROP TABLE IF EXISTS dist_test_prom_series ON CLUSTER abc SYNC"
curl "localhost:8123" -d "CREATE TABLE dist_test_prom_series ON CLUSTER abc AS test_prom_series ENGINE = Distributed(abc, default, test_prom_series);"

echo "send messages to kafka"
echo "cat /tmp/test_prom_metric.data | kafka-console-producer --topic test_metric_topic --broker-list localhost:9092" > send.sh
sudo docker cp ./docker/test_prom_metric.data kafka:/tmp/
sudo docker cp send.sh kafka:/tmp/
sudo docker exec kafka kafka-topics --bootstrap-server localhost:9093 --topic test_metric_topic --delete
sudo docker exec kafka sh /tmp/send.sh

echo "start clickhouse_sinker to consume"
timeout 30 ./clickhouse_sinker --local-cfg-file docker/test_prom_metric.hjson

schema=`curl "localhost:8123" -d 'DESC test_prom_metric' 2>/dev/null | sort | tr -d '\t' | tr -d ' '| tr '\n' ','`
echo "Got test_prom_metric schema => $schema"
[ $schema = "__series_idInt64,timestampDateTimeDoubleDelta,LZ4,value1Nullable(Float64),value2Nullable(Int64),value3Nullable(Bool),valueFloat32ZSTD(15)," ] || exit 1


schema=`curl "localhost:8123" -d 'DESC test_prom_series' 2>/dev/null | sort | tr -d '\t' | tr -d ' '| tr '\n' ','`
echo "Got test_prom_series schema => $schema"
[ $schema = "key_0Nullable(String),key_1Nullable(String),key_2Nullable(String),key_3Nullable(String),key_4Nullable(String),key_5Nullable(String),key_6Nullable(String),key_7Nullable(String),key_8Nullable(String),key_9Nullable(String),labelsString,__mgmt_idInt64,__name__String,__series_idInt64," ] || exit 1

echo "check result 1"
count=`curl "localhost:8123" -d 'select count() from dist_test_prom_metric'`
echo "Got test_prom_metric count => $count"
[ $count -le 10000 ] || exit 1

count=`curl "localhost:8123" -d 'select count() from dist_test_prom_series'`
echo "Got test_prom_series count => $count"
[ $count -eq 1000 ] || exit 1
#!/usr/bin/env bash

## create table
curl "localhost:8123" -d 'DROP TABLE IF EXISTS test1'
curl "localhost:8123" -d 'CREATE TABLE IF NOT EXISTS test1
(
    `day` Date DEFAULT toDate(time),
    `time` DateTime DEFAULT toDateTime(timestamp / 1000),
    `timestamp` UInt64,
    `name` String,
    `value` Float64
)
ENGINE = MergeTree
PARTITION BY day
ORDER BY time'

curl "localhost:8123" -d 'DROP TABLE IF EXISTS test_auto_schema'
curl "localhost:8123" -d 'CREATE TABLE IF NOT EXISTS test_auto_schema
(
    `day` Date DEFAULT toDate(time),
    `time` DateTime DEFAULT toDateTime(timestamp / 1000),
    `timestamp` UInt64,
    `name` String,
    `value` Float64
)
ENGINE = MergeTree
PARTITION BY day
ORDER BY time'


## send the messages to kafka
current_timestamp=`date +%s`000
for i in `seq 1 100000`;do
 echo "{\"timestamp\" : \"${current_timestamp}\", \"name\" : \"sundy-li\", \"value\" : \"$i\" }"
done > a.json
echo "cat /tmp/a.json | kafka-console-producer --topic topic1 --broker-list localhost:9092" > send.sh

sudo docker cp a.json kafka:/tmp/
sudo docker cp send.sh kafka:/tmp/
sudo docker exec kafka   sh /tmp/send.sh

## start clickhouse_sinker to consume
timeout 30 ./dist/clickhouse_sinker -conf docker/conf

## check result
count=`curl "localhost:8123" -d 'select count() from test1'`
echo "Got test1 count => $count"
[ $count -eq 100000 ] || exit 1

count=`curl "localhost:8123" -d 'select count() from test_auto_schema'`
echo "Got test_auto_schema count => $count"
[ $count -eq 100000 ] || exit 1


## reset kafka consumer-group offsets
echo "kafka-consumer-groups  --bootstrap-server localhost:9093 --execute --reset-offsets --group test_sinker --all-topics --to-earliest; kafka-consumer-groups  --bootstrap-server localhost:9093 --execute --reset-offsets --group test_auto_schema --all-topics --to-earliest" > reset-offsets.sh
sudo docker cp reset-offsets.sh kafka:/tmp/
sudo docker exec kafka   sh /tmp/reset-offsets.sh

## truncate tables
curl "localhost:8123" -d 'TRUNCATE TABLE test1'
curl "localhost:8123" -d 'TRUNCATE TABLE test_auto_schema'

## publish clickhouse_sinker config
./dist/nacos_publish_config --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos --sinker-conf docker/conf

## start clickhouse_sinker to consume
timeout 30 ./dist/clickhouse_sinker --nacos-register-enable --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos

## check result
count=`curl "localhost:8123" -d 'select count() from test1'`
echo "Got test1 count => $count"
[ $count -eq 100000 ] || exit 1

count=`curl "localhost:8123" -d 'select count() from test_auto_schema'`
echo "Got test_auto_schema count => $count"
[ $count -eq 100000 ] || exit 1

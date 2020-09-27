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

## start to consume
timeout 30 ./dist/clickhouse_sinker -conf docker/conf

count=`curl "localhost:8123" -d 'select count() from test1'`
echo "Got test1 count => $count"
[ $count -eq 100000 ] || exit 1


count=`curl "localhost:8123" -d 'select count() from test_auto_schema'`
echo "Got test_auto_schema count => $count"
[ $count -eq 100000 ] || exit 1

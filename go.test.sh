#!/bin/bash
make build

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

## send the messages to kafka
current_timestamp=`date +%s`000
for i in `seq 1 100000`;do
 echo "{\"timestamp\" : \"${current_timestamp}\", \"name\" : \"sundy-li\", \"value\" : \"$i\" }"
done > a.json
echo "cat /tmp/a.json | kafka-console-producer --topic topic1 --broker-list localhost:9092" > send.sh

sudo docker cp a.json clickhouse_sinker_kafka_1:/tmp/
sudo docker cp send.sh clickhouse_sinker_kafka_1:/tmp/
sudo docker exec clickhouse_sinker_kafka_1   sh /tmp/send.sh

## start to consume
nohup ./dist/clickhouse_sinker -conf docker/conf &

##wait for 30seconds, it should be enough
sleep 30
sudo kill -9 `pidof clickhouse_sinker`
count=`curl "localhost:8123" -d 'select count() from test1'`
[ $count -eq 100000 ] || exit 1
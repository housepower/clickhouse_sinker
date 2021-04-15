#!/usr/bin/env bash

echo "create tables"
curl "localhost:8123" -d 'DROP TABLE IF EXISTS test_fixed_schema'
curl "localhost:8123" -d 'CREATE TABLE test_fixed_schema
(
    `day` Date DEFAULT toDate(time),
    `time` DateTime,
    `name` String,
    `value` Float64
)
ENGINE = MergeTree
PARTITION BY day
ORDER BY (time, name)'

curl "localhost:8123" -d 'DROP TABLE IF EXISTS test_auto_schema'
curl "localhost:8123" -d 'CREATE TABLE test_auto_schema AS test_fixed_schema'

curl "localhost:8123" -d 'DROP TABLE IF EXISTS test_dynamic_schema'
curl "localhost:8123" -d 'CREATE TABLE test_dynamic_schema AS test_fixed_schema'

counts=`curl "localhost:8123" -d 'SELECT count() FROM test_fixed_schema UNION ALL SELECT count() FROM test_auto_schema UNION ALL SELECT count() FROM test_dynamic_schema' 2>/dev/null | tr '\n' ','`
echo "Got initial row counts => $counts"
[ $counts = "0,0,0," ] || exit 1

now=`date --rfc-3339=ns`
for i in `seq 1 10000`;do
    echo "{\"time\" : \"${now}\", \"name\" : \"name$i\", \"value\" : $i }"
done > a.json
for i in `seq 10001 30000`;do
    echo "{\"time\" : \"${now}\", \"name\" : \"name$i\", \"value\" : $i, \"newkey1\" : $i }"
done >> a.json
for i in `seq 30001 50000`;do
    echo "{\"time\" : \"${now}\", \"name\" : \"name$i\", \"value\" : $i, \"newkey2\" : $i.123 }"
done >> a.json
for i in `seq 50001 70000`;do
    echo "{\"time\" : \"${now}\", \"name\" : \"name$i\", \"value\" : $i, \"newkey3\" : \"name$i\" }"
done >> a.json
for i in `seq 70001 100000`;do
    echo "{\"time\" : \"${now}\", \"name\" : \"name$i\", \"value\" : $i }"
done >> a.json
echo "generated a.json"
echo "send messages to kafka"
echo "cat /tmp/a.json | kafka-console-producer --topic topic1 --broker-list localhost:9092" > send.sh
sudo docker cp a.json kafka:/tmp/
sudo docker cp send.sh kafka:/tmp/
sudo docker exec kafka sh /tmp/send.sh

echo "start clickhouse_sinker to consume"
timeout 30 ./dist/clickhouse_sinker --local-cfg-file docker/test_fixed_schema.json
timeout 30 ./dist/clickhouse_sinker --local-cfg-file docker/test_auto_schema.json
timeout 60 ./dist/clickhouse_sinker --local-cfg-file docker/test_dynamic_schema.json

echo "check result"
count=`curl "localhost:8123" -d 'select count() from test_fixed_schema'`
echo "Got test_fixed_schema count => $count"
[ $count -eq 100000 ] || exit 1

count=`curl "localhost:8123" -d 'select count() from test_auto_schema'`
echo "Got test_auto_schema count => $count"
[ $count -eq 100000 ] || exit 1

schema=`curl "localhost:8123" -d 'DESC test_dynamic_schema' 2>/dev/null | grep newkey | sort | tr -d '\t' | tr '\n' ','`
echo "Got test_dynamic_schema schema => $schema"
[ $schema = "newkey1Nullable(Int64),newkey2Nullable(Float64),newkey3Nullable(String)," ] || exit 1
count=`curl "localhost:8123" -d 'SELECT count() FROM test_dynamic_schema'`
echo "Got test_dynamic_schema count => $count"
[ $count -eq 100000 ] || exit 1


echo "reset kafka consumer-group offsets"
sudo docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9093 --execute --reset-offsets --group test_fixed_schema --all-topics --to-earliest
sudo docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9093 --execute --reset-offsets --group test_auto_schema --all-topics --to-earliest
sudo docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9093 --execute --reset-offsets --group test_dynamic_schema --all-topics --to-earliest

echo "truncate tables"
curl "localhost:8123" -d 'TRUNCATE TABLE test_fixed_schema'
curl "localhost:8123" -d 'TRUNCATE TABLE test_auto_schema'
curl "localhost:8123" -d 'TRUNCATE TABLE test_dynamic_schema'

echo "publish clickhouse_sinker config"
./dist/nacos_publish_config --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos  --nacos-dataid test_fixed_schema --local-cfg-file docker/test_fixed_schema.json
./dist/nacos_publish_config --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos  --nacos-dataid test_auto_schema --local-cfg-file docker/test_auto_schema.json
./dist/nacos_publish_config --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos  --nacos-dataid test_dynamic_schema --local-cfg-file docker/test_dynamic_schema.json

echo "start clickhouse_sinker to consume"
timeout 30 ./dist/clickhouse_sinker --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos --nacos-dataid test_fixed_schema
timeout 30 ./dist/clickhouse_sinker --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos --nacos-dataid test_auto_schema
timeout 30 ./dist/clickhouse_sinker --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos --nacos-dataid test_dynamic_schema

echo "check result"
count=`curl "localhost:8123" -d 'select count() from test_fixed_schema'`
echo "Got test_fixed_schema count => $count"
[ $count -eq 100000 ] || exit 1

count=`curl "localhost:8123" -d 'select count() from test_auto_schema'`
echo "Got test_auto_schema count => $count"
[ $count -eq 100000 ] || exit 1

count=`curl "localhost:8123" -d 'SELECT count() FROM test_dynamic_schema'`
echo "Got test_dynamic_schema count => $count"
[ $count -eq 100000 ] || exit 1

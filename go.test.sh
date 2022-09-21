#!/usr/bin/env bash

echo "create tables"
curl "localhost:8123" -d 'DROP TABLE IF EXISTS test_fixed_schema'
curl "localhost:8123" -d 'CREATE TABLE test_fixed_schema
(
    time DateTime,
    name String,
    value Float32,
    price Decimal32(3) DEFAULT(9.9)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(time)
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
    price=`echo "scale = 3; $i / 1000" | bc -q`
    echo "{\"time\" : \"${now}\", \"name\" : \"name$i\", \"value\" : $i, \"price\" : $price }"
done > a.json
for i in `seq 10001 30000`;do
    echo "{\"time\" : \"${now}\", \"name\" : \"name$i\", \"value\" : $i, \"newkey00\" : false, \"newkey01\" : $i }"
done >> a.json
for i in `seq 30001 50000`;do
    echo "{\"time\" : \"${now}\", \"name\" : \"name$i\", \"value\" : $i, \"newkey02\" : $i.123, \"newkey03\" : \"name$i\", \"newkey04\" : \"${now}\", \"newkey05\" : {\"k1\": 1, \"k2\": 2} }"
done >> a.json
for i in `seq 50001 70000`;do
    echo "{\"time\" : \"${now}\", \"name\" : \"name$i\", \"value\" : $i, \"newkey06\" : [$i], \"newkey07\" : [$i.123], \"newkey08\" : [\"name$i\"], \"newkey09\" : [\"${now}\"], \"newkey10\" : [{\"k1\": 1, \"k2\": 2}, {\"k3\": 3, \"k4\": 4}] }"
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
timeout 30 ./clickhouse_sinker --local-cfg-file docker/test_fixed_schema.json
timeout 30 ./clickhouse_sinker --local-cfg-file docker/test_auto_schema.json
timeout 60 ./clickhouse_sinker --local-cfg-file docker/test_dynamic_schema.json

echo "check result 1"
count=`curl "localhost:8123" -d 'select count() from test_fixed_schema'`
echo "Got test_fixed_schema count => $count"
[ $count -eq 100000 ] || exit 1

count=`curl "localhost:8123" -d 'select count() from test_auto_schema'`
echo "Got test_auto_schema count => $count"
[ $count -eq 100000 ] || exit 1

schema=`curl "localhost:8123" -d 'DESC test_dynamic_schema' 2>/dev/null | grep newkey | sort | tr -d '\t' | tr '\n' ','`
echo "Got test_dynamic_schema schema => $schema"
[ $schema = "newkey00Nullable(Bool),newkey01Nullable(Int64),newkey02Nullable(Float64),newkey03Nullable(String),newkey04Nullable(DateTime64(3))," ] || exit 1
count=`curl "localhost:8123" -d 'SELECT count() FROM test_dynamic_schema'`
echo "Got test_dynamic_schema count => $count"
[ $count -eq 100000 ] || exit 1

echo "truncate tables"
curl "localhost:8123" -d 'TRUNCATE TABLE test_fixed_schema'
curl "localhost:8123" -d 'TRUNCATE TABLE test_auto_schema'
curl "localhost:8123" -d 'TRUNCATE TABLE test_dynamic_schema'

echo "publish clickhouse_sinker config"
./nacos_publish_config --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos  --nacos-dataid test_fixed_schema --local-cfg-file docker/test_fixed_schema.json
./nacos_publish_config --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos  --nacos-dataid test_auto_schema --local-cfg-file docker/test_auto_schema.json
./nacos_publish_config --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos  --nacos-dataid test_dynamic_schema --local-cfg-file docker/test_dynamic_schema.json

echo "start clickhouse_sinker to consume"
sudo docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9093 --execute --reset-offsets --group test_fixed_schema --all-topics --to-earliest
timeout 30 ./clickhouse_sinker --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos --nacos-dataid test_fixed_schema

sudo docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9093 --execute --reset-offsets --group test_auto_schema --all-topics --to-earliest
timeout 30 ./clickhouse_sinker --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos --nacos-dataid test_auto_schema

sudo docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9093 --execute --reset-offsets --group test_dynamic_schema --all-topics --to-earliest
timeout 30 ./clickhouse_sinker --nacos-addr 127.0.0.1:8848 --nacos-username nacos --nacos-password nacos --nacos-dataid test_dynamic_schema

echo "check result 2"
count=`curl "localhost:8123" -d 'select count() from test_fixed_schema'`
echo "Got test_fixed_schema count => $count"
[ $count -eq 100000 ] || exit 1

count=`curl "localhost:8123" -d 'select count() from test_auto_schema'`
echo "Got test_auto_schema count => $count"
[ $count -eq 100000 ] || exit 1

count=`curl "localhost:8123" -d 'SELECT count() FROM test_dynamic_schema'`
echo "Got test_dynamic_schema count => $count"
[ $count -eq 100000 ] || exit 1

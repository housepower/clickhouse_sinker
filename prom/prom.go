package prom

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ClickhouseEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ch_sinker_clickhouse_events_total",
		Help: "The total number of events to insert into ClickHouse",
	},
		[]string{"db", "table"})

	ClickhouseEventsSuccess = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ch_sinker_clickhouse_events_success",
		Help: "The number of events successfully inserted into ClickHouse",
	},
		[]string{"db", "table"})

	ClickhouseEventsErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ch_sinker_clickhouse_events_errors",
		Help: "The number of events didn't inserted into ClickHouse",
	},
		[]string{"db", "table"})

	ClickhouseReconnectTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ch_sinker_clickhouse_reconnect_total",
		Help: "The total number of ClickHouse reconnects"})

	KafkaConsumerErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ch_sinker_kafka_consumer_errors",
		Help: "The number of kafka consumer errors",
	},
		[]string{"topic", "consumer_group"})
)

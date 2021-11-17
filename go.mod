module github.com/housepower/clickhouse_sinker

go 1.14

require (
	github.com/ClickHouse/clickhouse-go v1.5.1
	github.com/RoaringBitmap/roaring v0.9.4
	github.com/Shopify/sarama v1.30.0
	github.com/bytedance/sonic v1.0.0-rc
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/fagongzi/goetty v1.7.0
	github.com/google/gops v0.3.18
	github.com/jinzhu/copier v0.3.2
	github.com/nacos-group/nacos-sdk-go v1.0.7
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.26.0
	github.com/segmentio/kafka-go v0.4.22
	github.com/smartystreets/goconvey v1.6.4 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/tidwall/gjson v1.10.1
	github.com/troian/healthcheck v0.1.4-0.20200127040058-c373fb6a0dc1
	github.com/twmb/franz-go v1.2.4
	github.com/twmb/franz-go/plugin/kzap v1.0.0
	github.com/valyala/fastjson v1.6.3
	github.com/xdg-go/scram v1.0.2
	github.com/xdg/scram v1.0.3 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	go.uber.org/zap v1.19.1
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/net v0.0.0-20211020060615-d418f374d309 // indirect
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

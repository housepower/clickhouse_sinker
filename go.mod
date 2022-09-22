module github.com/housepower/clickhouse_sinker

go 1.19

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.3.0
	github.com/RoaringBitmap/roaring v1.2.1
	github.com/Shopify/sarama v1.36.0
	github.com/bytedance/sonic v1.5.0
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/fagongzi/goetty v1.7.0
	github.com/google/gops v0.3.25
	github.com/jcmturner/gokrb5/v8 v8.4.3
	github.com/jinzhu/copier v0.3.5
	github.com/nacos-group/nacos-sdk-go v1.1.2
	github.com/prometheus/client_golang v1.13.0
	github.com/prometheus/common v0.37.0
	github.com/segmentio/kafka-go v0.4.34
	github.com/shopspring/decimal v1.3.1
	github.com/stretchr/testify v1.8.0
	github.com/thanos-io/thanos v0.27.0
	github.com/tidwall/gjson v1.14.2
	github.com/troian/healthcheck v0.1.4-0.20200127040058-c373fb6a0dc1
	github.com/twmb/franz-go v1.7.1
	github.com/twmb/franz-go/pkg/kadm v1.2.1
	github.com/twmb/franz-go/pkg/sasl/kerberos v1.1.0
	github.com/twmb/franz-go/plugin/kzap v1.1.1
	github.com/valyala/fastjson v1.6.3
	github.com/xdg-go/scram v1.1.1
	go.uber.org/zap v1.22.0
	golang.org/x/exp v0.0.0-20220722155223-a9213eeb770e
	golang.org/x/time v0.0.0-20220722155302-e5dcc9cfc0b9
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

require (
	github.com/ClickHouse/ch-go v0.47.3 // indirect
	github.com/aliyun/alibaba-cloud-sdk-go v1.61.1723 // indirect
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.3.0 // indirect
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/chenzhuoyu/base64x v0.0.0-20220526154910-8bf9453eb81a // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.3.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/go-errors/errors v1.4.2 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.6.1 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/klauspost/cpuid/v2 v2.1.1 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/paulmach/orb v0.7.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/streadway/amqp v1.0.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.2.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/stringprep v1.0.3 // indirect
	github.com/xdg/scram v1.0.5 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	go.opentelemetry.io/otel v1.9.0 // indirect
	go.opentelemetry.io/otel/trace v1.9.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/arch v0.0.0-20220919183040-2926576b28c0 // indirect
	golang.org/x/crypto v0.0.0-20220817201139-bc19a97f63c8 // indirect
	golang.org/x/net v0.0.0-20220812174116-3211cb980234 // indirect
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4 // indirect
	golang.org/x/sys v0.0.0-20220919091848-fb04ddd9f9c8 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

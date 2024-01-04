FROM golang:1.21-alpine3.19 AS builder

ADD . /app
WORKDIR /app
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN make build

FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata && \
        echo "UTC" >  /etc/timezone
COPY --from=builder /app/bin/clickhouse_sinker /usr/local/bin/clickhouse_sinker
COPY --from=builder /app/bin/nacos_publish_config /usr/local/bin/nacos_publish_config
COPY --from=builder /app/bin/kafka_gen_log /usr/local/bin/kafka_gen_log
COPY --from=builder /app/bin/kafka_gen_metric /usr/local/bin/kafka_gen_metric

# clickhouse_sinker gets config from local file "/etc/clickhouse_sinker.hjson" by default.
# Customize behavior with following env variables:
# - V
# - LOG_LEVEL
# - LOG_PATHS
# - HTTP_PORT
# - HTTP_HOST
# - METRIC_PUSH_GATEWAY_ADDRS
# - PUSH_INTERVAL
# - LOCAL_CFG_FILE
# - NACOS_ADDR
# - NACOS_USERNAME
# - NACOS_PASSWORD
# - NACOS_NAMESPACE_ID
# - NACOS_GROUP
# - NACOS_DATAID
# - NACOS_SERVICE_NAME
# - CLICKHOUSE_USERNAME
# - CLICKHOUSE_PASSWORD
# - KAFKA_USERNAME
# - KAFKA_PASSWORD
# - KAFKA_GSSAPI_USERNAME
# - KAFKA_GSSAPI_PASSWORD
# See cmd/clickhouse_sinker/main.go for details.

ENTRYPOINT ["/usr/local/bin/clickhouse_sinker"]

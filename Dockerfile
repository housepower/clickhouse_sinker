FROM golang:latest AS builder

ADD . /app
WORKDIR /app
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN make build

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
RUN echo "UTC" >  /etc/timezone
COPY --from=builder /app/clickhouse_sinker /usr/local/bin/clickhouse_sinker
COPY --from=builder /app/nacos_publish_config /usr/local/bin/nacos_publish_config

# clickhouse_sinker gets config from local directory "/etc/clickhouse_sinker" by default.
# Customize behavior with following env variables:
# - V
# - HTTP_PORT
# - METRIC_PUSH_GATEWAY_ADDRS
# - PUSH_INTERVAL
# - LOCAL_CFG_FILE
# - NACOS_ADDR
# - NACOS_USERNAME
# - NACOS_PASSWORD
# - NACOS_NAMESPACE_ID
# - NACOS_GROUP
# - NACOS_DATAID
# See cmd/clickhouse_sinker/main.go for details.

ENTRYPOINT ["/usr/local/bin/clickhouse_sinker"]

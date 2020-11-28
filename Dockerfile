FROM golang:latest AS builder

ADD . /app
WORKDIR /app
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN make build

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
RUN echo "UTC" >  /etc/timezone
COPY --from=builder /app/dist/clickhouse_sinker /usr/local/bin/clickhouse_sinker
COPY --from=builder /app/dist/nacos_publish_config /usr/local/bin/nacos_publish_config

# clickhouse_sinker gets config from local directory "/etc/clickhouse_sinker" by default.
# Customize behavior with following env variables:
# - V
# - HTTP_PORT
# - METRIC_PUSH_GATEWAY_ADDRS
# - PUSH_INTERVAL
# - LOCAL_CFG_DIR
# - CONSUL_REGISTER_ENABLE
# - CONSUL_ADDR
# - CONSUL_DEREGISTER_CRITICAL_SERVICES_AFTER
# - NACOS_REGISTER_ENABLE
# - NACOS_ADDR
# - NACOS_NAMESPACE_ID
# - NACOS_GROUP
# - NACOS_USERNAME
# - NACOS_PASSWORD
# See cmd/clickhouse_sinker/main.go for details.

ENTRYPOINT ["/usr/local/bin/clickhouse_sinker"]

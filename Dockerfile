FROM golang:latest AS builder

ADD . /app
WORKDIR /app
RUN GOFLAGS='-mod=vendor' CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /clickhouse_sinker bin/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /clickhouse_sinker /usr/local/bin/clickhouse_sinker
RUN chmod +x /usr/local/bin/clickhouse_sinker
ENTRYPOINT ["/usr/local/bin/clickhouse_sinker"]

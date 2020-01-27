FROM golang:latest AS builder

ADD . /app
WORKDIR /app
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /clickhouse_sinker bin/main.go
RUN chmod +x /clickhouse_sinker

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
RUN echo "UTC" >  /etc/timezone
COPY --from=builder /clickhouse_sinker /usr/local/bin/clickhouse_sinker
ADD docker-entrypoint.sh /docker-entrypoint.sh
CMD ["/docker-entrypoint.sh"]

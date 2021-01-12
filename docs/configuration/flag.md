# run args

```
./clickhouse_sinker -h

Usage of ./clickhouse_sinker:
  -http-port int
        http listen port (default 2112)
  -local-cfg-file string
        local config file (default "/etc/clickhouse_sinker.json")
  -metric-push-gateway-addrs string
        a list of comma-separated prometheus push gatway address
  -nacos-addr string
        a list of comma-separated nacos server addresses (default "127.0.0.1:8848")
  -nacos-dataid string
        nacos dataid
  -nacos-group string
        nacos group name. Empty string doesn't work! (default "DEFAULT_GROUP")
  -nacos-namespace-id string
        nacos namespace ID. Neither DEFAULT_NAMESPACE_ID("public") nor namespace name work!
  -nacos-password string
        nacos password (default "nacos")
  -nacos-username string
        nacos username (default "nacos")
  -push-interval int
        push interval in seconds (default 10)
  -v    show build version and quit
```
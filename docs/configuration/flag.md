# run args

```
./clickhouse_sinker -h

Usage of ./dist/clickhouse_sinker:
  -consul-addr string
        consul api interface address (default "http://127.0.0.1:8500")
  -consul-deregister-critical-services-after string
        configure service check DeregisterCriticalServiceAfter (default "30m")
  -consul-register-enable
        register current instance in consul
  -http-port int
        http listen port (default 2112)
  -local-cfg-dir config.json
        local config dir. requires a file named config.json, and some task json files under `tasks` folder (default "/etc/clickhouse_sinker")
  -local-cfg-file string
        local config file (default "/etc/clickhouse_sinker.json")
  -metric-push-gateway-addrs string
        a list of comma-separated prometheus push gatway address
  -nacos-addr string
        a list of comma-separated nacos server addresses (default "127.0.0.1:8848")
  -nacos-group string
        nacos group name. Empty string doesn't work! (default "DEFAULT_GROUP")
  -nacos-namespace-id string
        nacos namespace ID. Neither DEFAULT_NAMESPACE_ID("public") nor namespace name work!
  -nacos-password string
        nacos password (default "nacos")
  -nacos-register-enable
        register current instance in nacos
  -nacos-username string
        nacos username (default "nacos")
  -push-interval int
        push interval in seconds (default 10)
  -v    show build version and quit
```
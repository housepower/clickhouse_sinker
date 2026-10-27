# run args

```
./clickhouse_sinker -h

Usage of ./clickhouse_sinker:
  -apollo-app-id string
    	Apollo appId
  -apollo-cluster string
    	Apollo cluster (default "default")
  -apollo-env string
    	Apollo env for the Open API, e.g. PRO
  -apollo-meta string
    	Apollo config service or meta server address
  -apollo-namespace string
    	Apollo txt/json namespace holding the config, e.g. clickhouse_sinker.txt
  -apollo-operator string
    	existing Apollo Portal user recorded as modifier and releaser
  -apollo-portal string
    	Apollo Portal address, enables write-back with the other --apollo-* Open API options
  -apollo-secret string
    	Apollo access key secret
  -apollo-token string
    	Apollo Open API token
  -clickhouse-password string
    	clickhouse password
  -clickhouse-username string
    	clickhouse username
  -e string
    	encrypt password
  -encrypt-algorithm string
    	algorithm for -e and for decrypting ENC() secrets: AESWITHHEXANDBASE64 or SM4 (SM4 needs the SINKER_SM4_KEY env) (default "AESWITHHEXANDBASE64")
  -etcd-ca-file string
    	etcd CA file, enables TLS
  -etcd-cert-file string
    	etcd client cert file, set with --etcd-key-file for mutual TLS
  -etcd-endpoints string
    	a list of comma-separated etcd endpoints
  -etcd-key string
    	etcd key holding the config
  -etcd-key-file string
    	etcd client key file
  -etcd-password string
    	etcd password
  -etcd-username string
    	etcd username
  -http-config-path string
	gjson path of the config inside a JSON response, e.g. "data.config"; empty means the response body is the config
  -http-data-id string
	query param appended to every config URL
  -http-group string
	query param appended to every config URL
  -http-headers string
	semicolon-separated "Key: Value" header pairs sent verbatim with every config request, e.g. "Authorization: Bearer xxx"
  -http-host string
    	http host to bind to
  -http-insecure-skip-verify
	skip TLS certificate verification for https config URLs
  -http-namespace string
	query param appended to every config URL
  -http-port int
    	http listen port
  -http-urls string
	a list of comma-separated config service URLs; each is GET in turn until one returns the config
  -kafka-gssapi-password string
    	kafka GSSAPI password
  -kafka-gssapi-username string
    	kafka GSSAPI username
  -kafka-password string
    	kafka password
  -kafka-username string
    	kafka username
  -local-cfg-file string
    	local config file (default "/etc/clickhouse_sinker.hjson")
  -log-level string
    	one of debug, info, warn, error, dpanic, panic, fatal (default "info")
  -log-paths string
    	a list of comma-separated log file path. stdout means the console stdout (default "stdout,clickhouse_sinker.log")
  -metric-push-gateway-addrs string
    	a list of comma-separated prometheus push gatway address
  -nacos-addr string
    	a list of comma-separated nacos server addresses (default "127.0.0.1:8848")
  -nacos-dataid string
    	nacos dataid
  -nacos-group string
    	nacos group name. Empty string doesn't work! (default "DEFAULT_GROUP")
  -nacos-namespace-id string
    	nacos namespace ID. Neither DEFAULT_NAMESPACE_ID("public") nor namespace name work! When namespace is 'public', fill in the blank string here!
  -nacos-password string
    	nacos password (default "nacos")
  -nacos-service-name string
    	deprecated and ignored, task assignment has been removed
  -nacos-username string
    	nacos username (default "nacos")
  -push-interval int
    	push interval in seconds (default 10)
  -v	show build version and quit
```

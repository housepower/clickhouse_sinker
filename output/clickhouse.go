package output

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"

	"github.com/wswz/go_commons/log"
	"github.com/wswz/go_commons/utils"
)

// ClickHouse is an output service consumers from kafka messages
type ClickHouse struct {
	conn *sql.DB

	Name      string
	TableName string
	Db        string
	Host      string
	Port      int

	// Clickhouse database config
	Clickhouse  string
	Username    string
	Password    string
	DsnParams   string
	MaxLifeTime time.Duration
	DnsLoop     bool

	// Table Configs
	Dims    []*ColumnWithType
	Metrics []*ColumnWithType

	prepareSQL string

	dmMap map[string]string
	dms   []string
}

// NewClickHouse new a clickhouse instance
func NewClickHouse() *ClickHouse {
	return &ClickHouse{}
}

// Init the clickhouse intance
func (c *ClickHouse) Init() error {
	return c.initAll()
}

// Write kvs to clickhouse
func (c *ClickHouse) Write(kvs []model.LogKV) (err error) {
	if len(kvs) == 0 {
		return
	}
	tx, err := c.conn.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(c.prepareSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, kv := range kvs {
		var args = make([]interface{}, len(c.dmMap))
		for i, name := range c.dms {
			args[i] = kv.GetValueByType(name, c.dmMap[name])
		}
		if _, err := stmt.Exec(args...); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return
	}
	return
}

func (c *ClickHouse) Close() error {
	return c.conn.Close()
}

func (c *ClickHouse) GetName() string {
	return c.Name
}

func (c *ClickHouse) Description() string {
	return "clickhouse desc"
}

func (c *ClickHouse) initAll() error {
	c.initConn()
	//根据 dms 生成prepare的sql语句
	c.dmMap = make(map[string]string)
	c.dms = make([]string, 0, len(c.Dims)+len(c.Metrics))
	for i, d := range c.Dims {
		c.dmMap[d.Name] = c.Dims[i].Type
		c.dms = append(c.dms, d.Name)
	}
	for i, m := range c.Metrics {
		c.dmMap[m.Name] = c.Metrics[i].Type
		c.dms = append(c.dms, m.Name)
	}
	var params = make([]string, len(c.dmMap))
	for i := range params {
		params[i] = "?"
	}
	c.prepareSQL = "INSERT INTO " + c.Db + "." + c.TableName + " (" + strings.Join(c.dms, ",") + ") VALUES (" + strings.Join(params, ",") + ")"

	log.Info("Prepare sql=>", c.prepareSQL)
	return nil
}

func (c *ClickHouse) initConn() (err error) {
	if conn, _ := pool.GetConn(c.Clickhouse); conn != nil {
		c.conn = conn
		return
	}

	var hosts []string
	if c.DnsLoop {
		ips, err := utils.GetIp4Byname(c.Host)
		if err != nil {
			return err
		}
		for _, ip := range ips {
			hosts = append(hosts, fmt.Sprintf("%s:%d", ip, c.Port))
		}
	} else {
		hosts = append(hosts, c.Host)
	}

	var dsn = fmt.Sprintf("tcp://%s?username=%s&password=%s", hosts[0], c.Username, c.Password)

	if len(hosts) > 1 {
		otherHosts := hosts[1:]
		dsn += "&alt_hosts="
		dsn += strings.Join(otherHosts, ",")
	}

	if c.DsnParams != "" {
		dsn += "&" + c.DsnParams
	}
	log.Info("clickhouse dsn", dsn)

	c.conn, err = pool.SetAndGetConn(dsn, c.Clickhouse)
	if err != nil {
		return
	}
	if c.MaxLifeTime > 0 {
		c.conn.SetConnMaxLifetime(time.Second * c.MaxLifeTime)
	}
	return
}

type ColumnWithType struct {
	Name string
	Type string
}

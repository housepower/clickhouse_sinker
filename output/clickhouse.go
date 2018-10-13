package output

import (
	"fmt"
	"strings"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/util"

	"github.com/wswz/go_commons/log"
	"github.com/wswz/go_commons/utils"
)

// ClickHouse is an output service consumers from kafka messages
type ClickHouse struct {
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
	Dims    []*model.ColumnWithType
	Metrics []*model.ColumnWithType

	prepareSQL string

	dmMap map[string]*model.ColumnWithType
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
func (c *ClickHouse) Write(metrics []model.Metric) (err error) {
	if len(metrics) == 0 {
		return
	}
	conn := pool.GetConn(c.Host)
	tx, err := conn.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(c.prepareSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, metric := range metrics {
		var args = make([]interface{}, len(c.dmMap))
		for i, name := range c.dms {
			args[i] = util.GetValueByType(metric, c.dmMap[name])
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

// LoopWrite will dead loop to write the records
func (c *ClickHouse) LoopWrite(metrics []model.Metric) {
	for err := c.Write(metrics); err != nil; {
		log.Error("saving msg error", err.Error(), "will loop to write the data")
		time.Sleep(3 * time.Second)
	}
}

func (c *ClickHouse) Close() error {
	return nil
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
	c.dmMap = make(map[string]*model.ColumnWithType)
	c.dms = make([]string, 0, len(c.Dims)+len(c.Metrics))
	for i, d := range c.Dims {
		c.dmMap[d.Name] = c.Dims[i]
		c.dms = append(c.dms, d.Name)
	}
	for i, m := range c.Metrics {
		c.dmMap[m.Name] = c.Metrics[i]
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
	var hosts []string

	// if contains ',', that means it's a ip list
	if strings.Contains(c.Host, ",") {
		hosts = strings.Split(strings.TrimSpace(c.Host), ",")
	} else {
		ips, err := utils.GetIp4Byname(c.Host)
		if err != nil {
			return err
		}
		for _, ip := range ips {
			hosts = append(hosts, fmt.Sprintf("%s:%d", ip, c.Port))
		}
	}

	var dsn = fmt.Sprintf("tcp://%s?username=%s&password=%s", hosts[0], c.Username, c.Password)
	if len(hosts) > 1 {
		otherHosts := hosts[1:]
		dsn += "&alt_hosts="
		dsn += strings.Join(otherHosts, ",")
		dsn += "&connection_open_strategy=random"
	}

	if c.DsnParams != "" {
		dsn += "&" + c.DsnParams
	}
	// dsn += "&debug=1"
	for i := 0; i < len(hosts); i++ {
		pool.SetDsn(c.Host, dsn)
	}
	return
}

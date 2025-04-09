package discovery

import (
	"fmt"
	"sort"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	cm "github.com/housepower/clickhouse_sinker/config_manager"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/util"
)

var (
	getClusterSQL string = `SELECT shard_num, replica_num, host_name FROM system.clusters WHERE cluster = '%s' ORDER BY shard_num, replica_num`
)

type Discovery struct {
	enabled bool
	config  *config.Config
	conn    *pool.Conn
	ncm     cm.RemoteConfManager
	rcm     bool
}

type Replicas []string
type Shards []Replicas

func (r Shards) Less(i, j int) bool {
	if len(r[i]) == 0 || len(r[j]) == 0 {
		return false
	}
	return r[i][0] < r[j][0]
}
func (r Shards) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
func (r Shards) Len() int {
	return len(r)
}

func NewDiscovery(cfg *config.Config, ncm cm.RemoteConfManager, rcm bool) *Discovery {
	return &Discovery{
		config: cfg,
		ncm:    ncm,
		rcm:    rcm,
	}
}

func (d *Discovery) SetConfig(conf config.Config) {
	d.config = &conf
	d.enabled = conf.Discovery.Enabled
}

func (d *Discovery) IsEnabled() bool {
	return d.enabled && d.rcm
}

func (d *Discovery) GetCKConn() error {
	// err := pool.InitClusterConn(&d.config.Clickhouse)
	// if err != nil {
	// 	return err
	// }
	conn, _, err := pool.GetShardConn(0).NextGoodReplica(d.config.Clickhouse.Ctx, 0)
	if err != nil {
		return err
	}
	d.conn = conn
	return nil
}

func (d *Discovery) Dispatcher() error {
	util.Logger.Debug("discovery: start")
	query := fmt.Sprintf(getClusterSQL, d.config.Clickhouse.Cluster)
	util.Logger.Debug("discovery: query: " + query)
	rows, err := d.conn.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	var shards Shards
	var replicas Replicas
	var lastShardNum uint32
	lastShardNum = 1
	for rows.Next() {
		var shardNum, replicaNum uint32
		var hostName string
		err = rows.Scan(&shardNum, &replicaNum, &hostName)
		if err != nil {
			return err
		}
		util.Logger.Debug(fmt.Sprintf("discovery: shardNum: %d, replicaNum: %d, hostName: %s", shardNum, replicaNum, hostName))
		if lastShardNum != shardNum {
			lastShardNum = shardNum
			shards = append(shards, replicas)
			replicas = make([]string, 0)
		}
		replicas = append(replicas, hostName)
		util.Logger.Debug(fmt.Sprintf("discovery: shards: %#v", shards))
		util.Logger.Debug(fmt.Sprintf("discovery: replicas: %#v", replicas))
	}
	if len(replicas) > 0 {
		shards = append(shards, replicas)
	}
	if len(shards) == 0 {
		return nil
	}
	util.Logger.Debug(fmt.Sprintf("discovery: shards: %#v", shards))
	if diffShards(shards, hosts2shards(d.config.Clickhouse.Hosts)) {
		util.Logger.Info(fmt.Sprintf("discovery: shards changed, old: %v, new: %v", d.config.Clickhouse.Hosts, shards))
		d.config.Clickhouse.Hosts = shards2hosts(shards)
		d.Publish()
	} else {
		util.Logger.Info("discovery: shards not changed")
	}
	return nil
}

func (d *Discovery) Publish() {
	d.config.Discovery.UpdatedBy = "discovery-changed"
	d.config.Discovery.UpdatedAt = time.Now()
	d.ncm.PublishConfig(d.config)
}

func hosts2shards(hosts [][]string) Shards {
	var shards Shards
	for _, host := range hosts {
		shards = append(shards, host)
	}
	return shards
}

func shards2hosts(shards Shards) [][]string {
	var hosts [][]string
	for _, shard := range shards {
		hosts = append(hosts, shard)
	}
	return hosts
}

func diffShards(old, new Shards) bool {
	if len(old) != len(new) {
		return true
	}
	// sort.Sort(old)
	// sort.Sort(new)
	for i := range old {
		if diffReplicas(old[i], new[i]) {
			return true
		}
	}
	return false
}

func diffReplicas(old, new []string) bool {
	if len(old) != len(new) {
		return true
	}
	sort.Sort(sort.StringSlice(old))
	sort.Sort(sort.StringSlice(new))
	for i := range old {
		if old[i] != new[i] {
			return true
		}
	}
	return false
}

package discovery

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	cm "github.com/housepower/clickhouse_sinker/config_manager"
	"github.com/housepower/clickhouse_sinker/pool"
	"github.com/housepower/clickhouse_sinker/util"
	"go.uber.org/zap"
)

var (
	getClusterSQL string = `SELECT shard_num, replica_num, host_name FROM system.clusters WHERE cluster = '%s' ORDER BY shard_num, replica_num`
)

type Discovery struct {
	enabled      bool
	config       *config.Config
	conn         *pool.Conn
	m            *cm.Manager
	warnReadOnly sync.Once
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

func NewDiscovery(cfg *config.Config, m *cm.Manager) *Discovery {
	return &Discovery{config: cfg, m: m}
}

func (d *Discovery) SetConfig(conf config.Config) {
	// conf is a shallow copy of the sinker's current config; own the hosts so
	// nothing here can reorder the caller's slices.
	conf.Clickhouse.Hosts = cloneHosts(conf.Clickhouse.Hosts)
	d.config = &conf
	d.enabled = conf.Discovery.Enabled
}

func (d *Discovery) IsEnabled() bool {
	if !d.enabled {
		return false
	}
	if !d.m.Writable() {
		// The discovery switch is hot-reloaded with the config, so this is
		// checked on every round rather than only at startup.
		d.warnReadOnly.Do(func() {
			util.Logger.Warn("discovery is enabled but the config backend is not writable, discovery is skipped")
		})
		return false
	}
	return true
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
	return d.applyShards(shards)
}

// applyShards writes discovered shards back to the config center. It patches
// only clickhouse.hosts on the latest config, so edits made by others since
// d.config was loaded survive.
func (d *Discovery) applyShards(shards Shards) error {
	if !diffShards(shards, hosts2shards(d.config.Clickhouse.Hosts)) {
		util.Logger.Info("discovery: shards not changed")
		return nil
	}
	util.Logger.Info(fmt.Sprintf("discovery: shards changed, old: %v, new: %v", d.config.Clickhouse.Hosts, shards))
	err := d.m.UpdateConfig(func(c *config.Config) bool {
		if !diffShards(shards, hosts2shards(c.Clickhouse.Hosts)) {
			return false // already written, e.g. by another sinker instance
		}
		c.Clickhouse.Hosts = shards2hosts(shards)
		c.Discovery.UpdatedBy = "discovery-changed"
		c.Discovery.UpdatedAt = time.Now()
		return true
	})
	if errors.Is(err, cm.ErrPendingChanges) {
		util.Logger.Warn("discovery: config center has unreleased changes, retry next round", zap.Error(err))
		return nil
	}
	return err
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
	for i := range old {
		if diffReplicas(old[i], new[i]) {
			return true
		}
	}
	return false
}

// diffReplicas compares replicas regardless of order without touching its
// arguments, which alias the sinker's current config.
func diffReplicas(old, new []string) bool {
	if len(old) != len(new) {
		return true
	}
	return !slices.Equal(slices.Sorted(slices.Values(old)), slices.Sorted(slices.Values(new)))
}

func cloneHosts(hosts [][]string) [][]string {
	if hosts == nil {
		return nil
	}
	out := make([][]string, len(hosts))
	for i, replicas := range hosts {
		out[i] = slices.Clone(replicas)
	}
	return out
}

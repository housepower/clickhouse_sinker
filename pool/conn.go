/*
Copyright [2019] housepower

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pool

// Clickhouse connection pool

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/health"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"github.com/troian/healthcheck"

	log "github.com/sirupsen/logrus"
)

const (
	BlockSize = 1 << 21 //2097152, two times of the default value
)

var (
	lock     sync.Mutex
	poolMaps map[string]*ClusterConnections
)

// Connection a datastructure for storing the clickhouse connection
type Connection struct {
	*sql.DB
	dsn string
}

type ClusterConnections struct {
	connections []*Connection
	ref         int
}

// ReConnect used for restablishing connection with server
func (c *Connection) ReConnect() error {
	sqlDB, err := sql.Open("clickhouse", c.dsn)
	if err != nil {
		log.Info("reconnect to ", c.dsn, err.Error())
		return err
	}
	setDBParams(sqlDB)
	log.Info("reconnect success to ", c.dsn)
	c.DB = sqlDB
	return nil
}

func InitConn(name string, hosts [][]string, port int, db, username, password, dsnParams string) (err error) {
	var sqlDB *sql.DB
	lock.Lock()
	if poolMaps == nil {
		poolMaps = make(map[string]*ClusterConnections)
	}
	if cc, ok := poolMaps[name]; ok {
		cc.ref++
		lock.Unlock()
		return
	}
	lock.Unlock()

	var cc ClusterConnections
	cc.ref = 1
	// Each shard has a *sql.DB which connects to all replicas inside the shard.
	// "alt_hosts" tolerates replica single-point-failure.
	for _, replicas := range hosts {
		numReplicas := len(replicas)
		replicaAddrs := make([]string, numReplicas)
		for i, ip := range replicas {
			if ips2, err := util.GetIp4Byname(ip); err == nil {
				ip = ips2[0]
			}
			replicaAddrs[i] = fmt.Sprintf("%s:%d", ip, port)
		}
		dsn := fmt.Sprintf("tcp://%s?database=%s&username=%s&password=%s&block_size=%d",
			replicaAddrs[0], db, username, password, BlockSize)
		if numReplicas > 1 {
			dsn += "&connection_open_strategy=in_order&alt_hosts=" + strings.Join(replicaAddrs[1:numReplicas], ",")
		}
		if dsnParams != "" {
			dsn += "&" + dsnParams
		}
		if sqlDB, err = sql.Open("clickhouse", dsn); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		setDBParams(sqlDB)
		cc.connections = append(cc.connections, &Connection{sqlDB, dsn})
	}

	lock.Lock()
	defer lock.Unlock()
	if cc2, ok := poolMaps[name]; ok {
		cc2.ref++
		return
	}
	for _, conn := range cc.connections {
		if err = health.Health.AddReadinessCheck(conn.dsn, healthcheck.DatabasePingCheck(conn.DB, 30*time.Second)); err != nil {
			err = errors.Wrapf(err, "")
			log.Errorf("got error: %+v", err)
		}
	}
	poolMaps[name] = &cc
	return nil
}

// TODO, pool this
func setDBParams(sqlDB *sql.DB) {
	sqlDB.SetMaxIdleConns(1)
	sqlDB.SetConnMaxLifetime(120 * time.Second)
}

func FreeConn(name string) {
	lock.Lock()
	defer lock.Unlock()
	if cc, ok := poolMaps[name]; ok {
		cc.ref--
		if cc.ref == 0 {
			delete(poolMaps, name)
			for _, conn := range cc.connections {
				if err := health.Health.RemoveReadinessCheck(conn.dsn); err != nil {
					err = errors.Wrapf(err, conn.dsn)
					log.Errorf("got error: %+v", err)
				}
			}
		}
	}
}

func GetTotalConn() (cnt int) {
	lock.Lock()
	defer lock.Unlock()
	for _, cc := range poolMaps {
		cnt += cc.ref * len(cc.connections)
	}
	return
}

// GetNumConn get number of connections for the given name
func GetNumConn(name string) (numConn int) {
	lock.Lock()
	defer lock.Unlock()
	if ps, ok := poolMaps[name]; ok {
		numConn = len(ps.connections)
	}
	return
}

// GetConn select a clickhouse node from the cluster based on batchNum
func GetConn(name string, batchNum int64) (con *Connection) {
	lock.Lock()
	defer lock.Unlock()

	cc, ok := poolMaps[name]
	if !ok {
		return
	}
	con = cc.connections[batchNum%int64(len(cc.connections))]
	return
}

// CloseAll closed all connection and destroys the pool
func CloseAll() {
	for _, cc := range poolMaps {
		for _, c := range cc.connections {
			_ = c.Close()
		}
	}
}

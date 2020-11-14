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

	"github.com/heptiolabs/healthcheck"
	"github.com/housepower/clickhouse_sinker/health"
	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
	"github.com/sundy-li/go_commons/utils"
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
	log.Info("reconnect success to ", c.dsn)
	c.DB = sqlDB
	return nil
}

func InitConn(name, hosts string, port int, db, username, password, dsnParams string) (err error) {
	var ips, ips2, dsnArr []string
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
	// if contains ',', that means it's a ip list
	if strings.Contains(hosts, ",") {
		ips = strings.Split(strings.TrimSpace(hosts), ",")
	} else {
		ips = []string{hosts}
	}
	for _, ip := range ips {
		if ips2, err = utils.GetIp4Byname(ip); err != nil {
			// fallback to ip
			err = nil
		} else {
			ip = ips2[0]
		}
		dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s&block_size=%d",
			ip, port, db, username, password, BlockSize)
		if dsnParams != "" {
			dsn += "&" + dsnParams
		}
		dsnArr = append(dsnArr, dsn)
	}

	log.Infof("clickhouse dsn of %s: %+v", name, dsnArr)
	var cc ClusterConnections
	cc.ref = 1
	for _, dsn := range dsnArr {
		if sqlDB, err = sql.Open("clickhouse", dsn); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		health.Health.AddReadinessCheck(dsn, healthcheck.DatabasePingCheck(sqlDB, 10*time.Second))
		cc.connections = append(cc.connections, &Connection{sqlDB, dsn})
	}
	lock.Lock()
	defer lock.Unlock()
	if cc2, ok := poolMaps[name]; ok {
		cc2.ref++
		return
	}
	poolMaps[name] = &cc
	return nil
}

func FreeConn(name string) {
	lock.Lock()
	defer lock.Unlock()
	if cc, ok := poolMaps[name]; ok {
		cc.ref--
		if cc.ref <= 0 {
			delete(poolMaps, name)
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

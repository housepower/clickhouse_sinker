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
	lock        sync.Mutex
	connections []*Connection
)

// Connection a datastructure for storing the clickhouse connection
type Connection struct {
	*sql.DB
	dsn string
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

func InitConn(hosts [][]string, port int, db, username, password, dsnParams string) (err error) {
	var sqlDB *sql.DB
	lock.Lock()
	defer lock.Unlock()
	// Each shard has a *sql.DB which connects to all replicas inside the shard.
	// "alt_hosts" tolerates replica single-point-failure.
	for _, replicas := range hosts {
		numReplicas := len(replicas)
		replicaAddrs := make([]string, numReplicas)
		for i, ip := range replicas {
			if ips2, err := util.GetIP4Byname(ip); err == nil {
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
		if err = health.Health.AddReadinessCheck(dsn, healthcheck.DatabasePingCheck(sqlDB, 30*time.Second)); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		connections = append(connections, &Connection{sqlDB, dsn})
	}
	return
}

// TODO: ClickHouse creates a thread for each TCP/HTTP connection.
// If the number of sinkers is close to clickhouse max_concurrent_queries(default 100), user queries could be blocked or refused.
func setDBParams(sqlDB *sql.DB) {
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(0)
	sqlDB.SetConnMaxIdleTime(10 * time.Second)
}

func FreeConn() {
	lock.Lock()
	defer lock.Unlock()
	for _, conn := range connections {
		if err := health.Health.RemoveReadinessCheck(conn.dsn); err != nil {
			err = errors.Wrapf(err, conn.dsn)
			log.Errorf("got error: %+v", err)
		}
		conn.DB.Close()
	}
	connections = []*Connection{}
}

func GetTotalConn() (cnt int) {
	lock.Lock()
	defer lock.Unlock()
	return len(connections)
}

// GetConn select a clickhouse node from the cluster based on batchNum
func GetConn(batchNum int64) (con *Connection) {
	lock.Lock()
	defer lock.Unlock()
	con = connections[batchNum%int64(len(connections))]
	return
}

// CloseAll closed all connection and destroys the pool
func CloseAll() {
	FreeConn()
}

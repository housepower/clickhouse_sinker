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
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/housepower/clickhouse_sinker/health"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/pkg/errors"
	"github.com/troian/healthcheck"
	"go.uber.org/zap"
)

const (
	BlockSize = 1 << 21 //2097152, two times of the default value
)

var (
	lock        sync.Mutex
	clusterConn []*ShardConn
	dsnTmpl     string
)

// ShardConn a datastructure for storing the clickhouse connection
type ShardConn struct {
	*sql.DB
	Dsn      string
	Replicas []string //ip:port list of replicas
	NextRep  int      //index of next replica
}

// NextGoodReplica connects to next good replica
func (c *ShardConn) NextGoodReplica() error {
	if c.DB != nil {
		if err := health.Health.RemoveReadinessCheck(c.Dsn); err != nil {
			util.Logger.Warn("health.Health.RemoveReadinessCheck failed", zap.String("dsn", c.Dsn), zap.Error(err))
		}
		c.DB.Close()
		c.DB = nil
	}
	savedNextRep := c.NextRep
	// try all replicas, including the current one
	for i := 0; i < len(c.Replicas); i++ {
		c.Dsn = fmt.Sprintf(dsnTmpl, c.Replicas[c.NextRep])
		c.NextRep = (c.NextRep + 1) % len(c.Replicas)
		util.Logger.Info("sql.Open", zap.String("dsn", c.Dsn))
		sqlDB, err := sql.Open("clickhouse", c.Dsn)
		if err != nil {
			util.Logger.Warn("sql.Open failed", zap.String("dsn", c.Dsn), zap.Error(err))
			continue
		}
		setDBParams(sqlDB)
		util.Logger.Info("sql.Open succeeded", zap.String("dsn", c.Dsn))
		if err = health.Health.AddReadinessCheck(c.Dsn, healthcheck.DatabasePingCheck(sqlDB, 30*time.Second)); err != nil {
			util.Logger.Warn("health.Health.RemoveReadinessCheck failed", zap.String("dsn", c.Dsn), zap.Error(err))
		}
		c.DB = sqlDB
		return nil
	}
	err := errors.Errorf("no good replica among replicas %v since %d", c.Replicas, savedNextRep)
	return err
}

func InitClusterConn(hosts [][]string, port int, db, username, password, dsnParams string, secure, skipVerify bool) (err error) {
	lock.Lock()
	defer lock.Unlock()
	freeClusterConn()
	// Each shard has a *sql.DB which connects to one replica inside the shard.
	// "alt_hosts" tolerates replica single-point-failure. However more flexable switching is needed for some cases for example https://github.com/ClickHouse/ClickHouse/issues/24036.
	dsnTmpl = "tcp://%s" + fmt.Sprintf("?database=%s&username=%s&password=%s&block_size=%d",
		url.QueryEscape(db), url.QueryEscape(username), url.QueryEscape(password), BlockSize)
	if dsnParams != "" {
		dsnTmpl += "&" + dsnParams
	}
	if secure {
		dsnTmpl += "&secure=true&skip_verify=" + strconv.FormatBool(skipVerify)
	}

	for _, replicas := range hosts {
		numReplicas := len(replicas)
		replicaAddrs := make([]string, numReplicas)
		for i, ip := range replicas {
			if ips2, err := util.GetIP4Byname(ip); err == nil {
				ip = ips2[0]
			}
			replicaAddrs[i] = fmt.Sprintf("%s:%d", ip, port)
		}
		sc := &ShardConn{
			Replicas: replicaAddrs,
		}
		if err = sc.NextGoodReplica(); err != nil {
			return
		}
		clusterConn = append(clusterConn, sc)
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

func freeClusterConn() {
	for _, sc := range clusterConn {
		if sc.DB != nil {
			if err := health.Health.RemoveReadinessCheck(sc.Dsn); err != nil {
				util.Logger.Error("health.Health.RemoveReadinessCheck failed", zap.String("dsn", sc.Dsn), zap.Error(err))
			}
			sc.DB.Close()
		}
	}
	clusterConn = []*ShardConn{}
}

func FreeClusterConn() {
	lock.Lock()
	defer lock.Unlock()
	freeClusterConn()
}

func NumShard() (cnt int) {
	lock.Lock()
	defer lock.Unlock()
	return len(clusterConn)
}

// GetShardConn select a clickhouse shard based on batchNum
func GetShardConn(batchNum int64) (con *ShardConn) {
	lock.Lock()
	defer lock.Unlock()
	con = clusterConn[batchNum%int64(len(clusterConn))]
	return
}

// CloseAll closed all connection and destroys the pool
func CloseAll() {
	FreeClusterConn()
}

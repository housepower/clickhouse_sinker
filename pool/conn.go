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
	lock     sync.Mutex
	db       *sql.DB
	connAt   time.Time
	dsn      string
	replicas []string //ip:port list of replicas
	nextRep  int      //index of next replica
}

// GetCurReplica returns the current replica connection
func (sc *ShardConn) GetCurReplica() (db *sql.DB) {
	sc.lock.Lock()
	db = sc.db
	sc.lock.Unlock()
	return
}

// Close closes the current replica connection
func (sc *ShardConn) Close() {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	if sc.db != nil {
		sc.db.Close()
		sc.db = nil
		if err := health.Health.RemoveReadinessCheck(sc.dsn); err != nil {
			util.Logger.Error("health.Health.RemoveReadinessCheck failed", zap.String("dsn", sc.dsn), zap.Error(err))
		}
	}
}

// NextGoodReplica connects to next good replica
func (sc *ShardConn) NextGoodReplica(failAt time.Time) (db *sql.DB, err error) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	if sc.db != nil {
		if sc.connAt.After(failAt) {
			// Another goroutine has already done connection.
			return sc.db, nil
		}
		if err := health.Health.RemoveReadinessCheck(sc.dsn); err != nil {
			util.Logger.Warn("health.Health.RemoveReadinessCheck failed", zap.String("dsn", sc.dsn), zap.Error(err))
		}
		sc.db.Close()
		sc.db = nil
	}
	savedNextRep := sc.nextRep
	// try all replicas, including the current one
	for i := 0; i < len(sc.replicas); i++ {
		sc.dsn = fmt.Sprintf(dsnTmpl, sc.replicas[sc.nextRep])
		sc.nextRep = (sc.nextRep + 1) % len(sc.replicas)
		sqlDB, err := sql.Open("clickhouse", sc.dsn)
		if err != nil {
			util.Logger.Warn("sql.Open failed", zap.String("dsn", sc.dsn), zap.Error(err))
			continue
		}
		// According to sql.Open doc, "Open may just validate its arguments without creating a connection
		// to the database. To verify that the data source name is valid, call Ping."
		if err := sqlDB.Ping(); err != nil {
			util.Logger.Warn("sqlDB.Ping failed", zap.String("dsn", sc.dsn), zap.Error(err))
			continue
		}
		setDBParams(sqlDB)
		util.Logger.Info("sql.Open and sqlDB.Ping succeeded", zap.String("dsn", sc.dsn))
		if err = health.Health.AddReadinessCheck(sc.dsn, healthcheck.DatabasePingCheck(sqlDB, 30*time.Second)); err != nil {
			util.Logger.Warn("health.Health.AddReadinessCheck failed", zap.String("dsn", sc.dsn), zap.Error(err))
		}
		sc.db = sqlDB
		sc.connAt = time.Now()
		return sc.db, nil
	}
	err = errors.Errorf("no good replica among replicas %v since %d", sc.replicas, savedNextRep)
	return nil, err
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
			replicas: replicaAddrs,
		}
		if _, err = sc.NextGoodReplica(time.Now()); err != nil {
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
		sc.Close()
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
func GetShardConn(batchNum int64) (sc *ShardConn) {
	lock.Lock()
	defer lock.Unlock()
	sc = clusterConn[batchNum%int64(len(clusterConn))]
	return
}

// CloseAll closed all connection and destroys the pool
func CloseAll() {
	FreeClusterConn()
}

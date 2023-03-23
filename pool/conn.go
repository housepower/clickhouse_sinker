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
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/thanos-io/thanos/pkg/errors"
	"go.uber.org/zap"
)

var (
	lock        sync.Mutex
	clusterConn []*ShardConn
)

// ShardConn a datastructure for storing the clickhouse connection
type ShardConn struct {
	lock        sync.Mutex
	db          clickhouse.Conn
	dbVer       int
	opts        clickhouse.Options
	replicas    []string         //ip:port list of replicas
	nextRep     int              //index of next replica
	writingPool *util.WorkerPool //the all tasks' writing ClickHouse, cpu-net balance
}

func (sc *ShardConn) SubmitTask(fn func()) (err error) {
	return sc.writingPool.Submit(fn)
}

// GetReplica returns the replica to which db connects
func (sc *ShardConn) GetReplica() (replica string) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	if sc.db != nil {
		curRep := (len(sc.replicas) + sc.nextRep - 1) % len(sc.replicas)
		replica = sc.replicas[curRep]
	}
	return
}

// Close closes the current replica connection
func (sc *ShardConn) Close() {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	if sc.db != nil {
		sc.db.Close()
		sc.db = nil
	}
	if sc.writingPool != nil {
		sc.writingPool.StopWait()
	}
}

// NextGoodReplica connects to next good replica
func (sc *ShardConn) NextGoodReplica(failedVer int) (db clickhouse.Conn, dbVer int, err error) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	if sc.db != nil {
		if sc.dbVer > failedVer {
			// Another goroutine has already done connection.
			// Notice: Why recording failure version instead timestamp?
			// Consider following scenario:
			// conn1 = NextGood(0); conn2 = NexGood(0); conn1.Exec failed at ts1;
			// conn3 = NextGood(ts1); conn2.Exec failed at ts2;
			// conn4 = NextGood(ts2) will close the good connection and break users.
			return sc.db, sc.dbVer, nil
		}
		sc.db.Close()
		sc.db = nil
	}
	savedNextRep := sc.nextRep
	// try all replicas, including the current one
	for i := 0; i < len(sc.replicas); i++ {
		replica := sc.replicas[sc.nextRep]
		sc.opts.Addr = []string{replica}
		sc.nextRep = (sc.nextRep + 1) % len(sc.replicas)
		sc.db, err = clickhouse.Open(&sc.opts)
		if err != nil {
			util.Logger.Warn("clickhouse.Open failed", zap.String("replica", replica), zap.Error(err))
			continue
		}
		sc.dbVer++
		util.Logger.Info("clickhouse.Open succeeded", zap.Int("dbVer", sc.dbVer), zap.String("replica", replica))
		return sc.db, sc.dbVer, nil
	}
	err = errors.Newf("no good replica among replicas %v since %d", sc.replicas, savedNextRep)
	return nil, sc.dbVer, err
}

// Each shard has a clickhouse.Conn which connects to one replica inside the shard.
// We need more control than replica single-point-failure.
func InitClusterConn(hosts [][]string, port int, db, username, password, dsnParams string, secure, skipVerify bool,
	maxOpenConns int, dialTimeout int) (err error) {
	lock.Lock()
	defer lock.Unlock()
	freeClusterConn()

	for _, replicas := range hosts {
		numReplicas := len(replicas)
		replicaAddrs := make([]string, numReplicas)
		for i, ip := range replicas {
			// Changing hostnames to IPs breaks TLS connections in many cases
			if !secure {
				if ips2, err := util.GetIP4Byname(ip); err == nil {
					ip = ips2[0]
				}
			}
			replicaAddrs[i] = fmt.Sprintf("%s:%d", ip, port)
		}
		sc := &ShardConn{
			replicas: replicaAddrs,
			opts: clickhouse.Options{
				Auth: clickhouse.Auth{
					Database: db,
					Username: username,
					Password: password,
				},
				DialTimeout:     time.Second * time.Duration(dialTimeout),
				MaxOpenConns:    maxOpenConns,
				MaxIdleConns:    5, // TODO - update this property to maxOpenConns when the lifetime of an idle connection honours the ConnMaxLifetime
				ConnMaxLifetime: time.Minute * 10,
			},
			writingPool: util.NewWorkerPool(maxOpenConns, 1),
		}
		if secure {
			tlsConfig := &tls.Config{}
			tlsConfig.InsecureSkipVerify = skipVerify
			sc.opts.TLS = tlsConfig
		}
		if _, _, err = sc.NextGoodReplica(0); err != nil {
			return
		}
		clusterConn = append(clusterConn, sc)
	}
	return
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

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
	"math/rand"
	"sync"
	"time"

	"github.com/heptiolabs/healthcheck"
	"github.com/housepower/clickhouse_sinker/health"

	"github.com/sundy-li/go_commons/log"
)

var (
	connNum = 1
	lock    sync.Mutex
)

// Connection a datastructure for storing the clickhouse connection
type Connection struct {
	*sql.DB
	Dsn string
}

// ReConnect used for restablishing connection with server
func (c *Connection) ReConnect() error {
	sqlDB, err := sql.Open("clickhouse", c.Dsn)
	if err != nil {
		log.Info("reconnect to ", c.Dsn, err.Error())
		return err
	}
	log.Info("reconnect success to  ", c.Dsn)
	c.DB = sqlDB
	return nil
}

var poolMaps = map[string][]*Connection{}

// SetDsn set the dsn for the connection
func SetDsn(name string, dsn string, maxLifetTime time.Duration) {
	lock.Lock()
	defer lock.Unlock()

	sqlDB, err := sql.Open("clickhouse", dsn)
	if err != nil {
		panic(err)
	}

	if maxLifetTime.Seconds() != 0 {
		sqlDB.SetConnMaxLifetime(maxLifetTime)
	}

	if ps, ok := poolMaps[name]; ok {
		//达到最大限制了，不需要新建conn
		if len(ps) >= connNum {
			return
		}
		log.Info("clickhouse dsn", dsn)
		ps = append(ps, &Connection{sqlDB, dsn})
		poolMaps[name] = ps
	} else {
		poolMaps[name] = []*Connection{{sqlDB, dsn}}
	}

	var ix int
	var i *Connection
	for ix, i = range poolMaps[name] {
		var checkName = fmt.Sprintf("clickhouse(%s, %d)", name, ix)
		health.Health.AddReadinessCheck(checkName, healthcheck.DatabasePingCheck(i.DB, 1*time.Second))
	}
}

// GetConn returns a connection for a clickhouse server from the pool
func GetConn(name string) *Connection {
	lock.Lock()
	defer lock.Unlock()

	ps := poolMaps[name]
	return ps[rand.Intn(len(ps))]
}

// CloseAll closed all connection and destroys the pool
func CloseAll() {
	for _, ps := range poolMaps {
		for _, c := range ps {
			_ = c.Close()
		}
	}
}

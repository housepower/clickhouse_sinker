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

	"github.com/sundy-li/go_commons/log"
	"github.com/sundy-li/go_commons/utils"
)

var (
	lock sync.Mutex
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

func InitConn(name, hosts string, port int, db, username, password, dsnParams string) (err error) {
	var ips, ips2, dsnArr []string
	var sqlDB *sql.DB
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
		dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
			ip, port, db, username, password)
		if dsnParams != "" {
			dsn += "&" + dsnParams
		}
		dsnArr = append(dsnArr, dsn)
	}

	log.Infof("clickhouse dsn of %s: %+v", name, dsnArr)
	var cons []*Connection
	for _, dsn := range dsnArr {
		if sqlDB, err = sql.Open("clickhouse", dsn); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		health.Health.AddReadinessCheck(dsn, healthcheck.DatabasePingCheck(sqlDB, 10*time.Second))
		cons = append(cons, &Connection{sqlDB, dsn})
	}
	lock.Lock()
	poolMaps[name] = cons
	lock.Unlock()
	return nil
}

// GetNumConn get number of connections for the given name
func GetNumConn(name string) (numConn int) {
	lock.Lock()
	defer lock.Unlock()
	if ps, ok := poolMaps[name]; ok {
		numConn = len(ps)
	}
	return
}

// GetConn select a clickhouse node from the cluster based on batchNum
func GetConn(name string, batchNum int64) (con *Connection) {
	lock.Lock()
	defer lock.Unlock()

	ps, ok := poolMaps[name]
	if !ok {
		return
	}
	con = ps[batchNum%int64(len(ps))]
	return
}

// CloseAll closed all connection and destroys the pool
func CloseAll() {
	for _, ps := range poolMaps {
		for _, c := range ps {
			_ = c.Close()
		}
	}
}

package pool

import (
	"database/sql"
	"errors"
	"sync"

	"github.com/wswz/go_commons/log"
)

var (
	conns    = map[string]*sql.DB{}
	connLock sync.Mutex
)

func SetAndGetConn(dsn string, name string) (conn *sql.DB, err error) {
	connLock.Lock()
	defer connLock.Unlock()

	if p, ok := conns[name]; ok {
		return p, nil
	}

	log.Info("init clickhouse dsn", dsn)
	sqlDB, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return
	}
	conns[name] = sqlDB
	return sqlDB, nil
}

func GetConn(name string) (conn *sql.DB, err error) {
	connLock.Lock()
	defer connLock.Unlock()

	if p, ok := conns[name]; ok {
		return p, nil
	}
	return nil, ErrConnNotFound
}

func CloseAll() {
	connLock.Lock()
	for _, conn := range conns {
		conn.Close()
	}
	connLock.Unlock()
}

var (
	ErrConnNotFound = errors.New("connection not found")
)

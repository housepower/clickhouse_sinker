// sql_pool.go
package pool

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/housepower/clickhouse_sinker/util"
	"go.uber.org/zap"
)

type SQLPoolManager struct {
	baseOpts *clickhouse.Options
	pools    *expirable.LRU[uint64, *sql.DB] // 使用哈希值作为key
	maxPools int
	poolTTL  time.Duration
}

func NewSQLPoolManager(baseOpts *clickhouse.Options) *SQLPoolManager {
	maxPools := 100
	poolTTL := 1 * time.Hour
	lru := expirable.NewLRU[uint64, *sql.DB](maxPools, nil, poolTTL)
	return &SQLPoolManager{
		baseOpts: baseOpts,
		maxPools: maxPools,
		pools:    lru,
		poolTTL:  poolTTL,
	}
}

func NewKey(sql string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(sql))
	return hash.Sum64()
}

func (m *SQLPoolManager) Get(sql string) (*sql.DB, error) {
	key := NewKey(sql)
	if pool, exists := m.pools.Get(key); exists {
		util.Logger.Debug("Get existing SQL pool",
			zap.String("sql_hash", fmt.Sprintf("%x", key)),
			zap.String("sql", sql))
		if err := pool.Ping(); err != nil {
			util.Logger.Warn("Existing pool connection is invalid, creating new one",
				zap.String("sql_hash", fmt.Sprintf("%x", key)),
				zap.Error(err))

			m.pools.Remove(key)
		} else {
			return pool, nil
		}
	}

	pool := clickhouse.OpenDB(&clickhouse.Options{
		Addr:             m.baseOpts.Addr,
		Auth:             m.baseOpts.Auth,
		DialTimeout:      m.baseOpts.DialTimeout,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		Protocol:         clickhouse.HTTP,
	})

	pool.SetMaxOpenConns(5)
	pool.SetMaxIdleConns(2)
	pool.SetConnMaxLifetime(0)
	pool.SetConnMaxIdleTime(30 * time.Second)

	m.pools.Add(key, pool)

	util.Logger.Debug("Created new SQL pool",
		zap.String("sql_hash", fmt.Sprintf("%x", key)),
		zap.String("sql", sql))

	return pool, nil
}

func (m *SQLPoolManager) Close() error {
	var lastErr error

	for _, key := range m.pools.Keys() {
		if pool, exists := m.pools.Peek(key); exists {
			if err := pool.Close(); err != nil {
				lastErr = err
			}
		}
	}
	return lastErr
}

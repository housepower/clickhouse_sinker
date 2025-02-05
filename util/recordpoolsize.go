package util

import "sync/atomic"

type RecordSize struct {
	poolSize int64
	realSize int64
}

func (rs *RecordSize) SetPoolSize(size int64) {
	rs.poolSize = size
}

func (rs *RecordSize) Inc(size int64) {
	atomic.AddInt64(&rs.realSize, size)
}

func (rs *RecordSize) Reset() {
	atomic.StoreInt64(&rs.realSize, 0)
}

func (rs *RecordSize) Dec(size int64) {
	atomic.AddInt64(&rs.realSize, size*(-1))
}

func (rs *RecordSize) Get() int64 {
	return atomic.LoadInt64(&rs.realSize)
}

func (rs *RecordSize) Allow() bool {
	realSize := atomic.LoadInt64(&rs.realSize)
	return realSize < rs.poolSize
}

var Rs RecordSize

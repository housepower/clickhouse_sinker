package util

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerPool is a blocked worker pool inspired by https://github.com/gammazero/workerpool/
type WorkerPool struct {
	inNums  uint64
	outNums uint64

	maxWorkers int
	workChan   chan struct{}

	stopOnce sync.Once
	stopped  int32
}

// New creates and starts a pool of worker goroutines.
func NewWorkerPool(maxWorkers int) *WorkerPool {
	return &WorkerPool{
		maxWorkers: maxWorkers,
		workChan:   make(chan struct{}, maxWorkers),
	}
}

var (
	// ErrorStopped when stopped
	ErrorStopped = errors.New("WorkerPool already stopped")
)

// Submit enqueues a function for a worker to execute.
// Submit will block regardless if there is no free workers.
func (w *WorkerPool) Submit(fn func()) (err error) {
	if atomic.LoadInt32(&w.stopped) == 1 {
		return ErrorStopped
	}

	atomic.AddUint64(&w.inNums, 1)
	w.workChan <- struct{}{}
	go func() {
		fn()
		<-w.workChan
		atomic.AddUint64(&w.outNums, 1)
	}()
	return nil
}

// StopWait stops the worker pool and waits for all queued tasks tasks to complete.
func (w *WorkerPool) StopWait() {
	w.stopOnce.Do(func() {
		atomic.StoreInt32(&w.stopped, 1)
	})

	for atomic.LoadUint64(&w.inNums) > atomic.LoadUint64(&w.outNums) {
		time.Sleep(3 * time.Millisecond)
	}
}

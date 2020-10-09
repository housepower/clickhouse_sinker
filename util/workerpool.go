package util

import (
	"errors"
	"sync"
	"sync/atomic"
)

const (
	StateRunning uint32 = 0
	StateStopped uint32 = 1
)

// WorkerPool is a blocked worker pool inspired by https://github.com/gammazero/workerpool/
type WorkerPool struct {
	inNums  uint64
	outNums uint64

	maxWorkers int
	workChan   chan func()

	taskDone *sync.Cond
	state    uint32
	sync.Mutex
}

// New creates and starts a pool of worker goroutines.
func NewWorkerPool(maxWorkers int) *WorkerPool {
	w := &WorkerPool{
		maxWorkers: maxWorkers,
		workChan:   make(chan func(), maxWorkers),
	}

	w.taskDone = sync.NewCond(w)

	go w.start()
	return w
}

var (
	// ErrorStopped when stopped
	ErrorStopped = errors.New("WorkerPool already stopped")
)

func (w *WorkerPool) start() {
	for i := 0; i < w.maxWorkers; i++ {
		go func() {
			for fn := range w.workChan {
				fn()

				w.Lock()
				w.outNums++
				if w.inNums == w.outNums {
					w.taskDone.Signal()
				}
				w.Unlock()
			}
		}()
	}
}

// Submit enqueues a function for a worker to execute.
// Submit will block regardless if there is no free workers.
func (w *WorkerPool) Submit(fn func()) (err error) {
	if atomic.LoadUint32(&w.state) == StateStopped {
		return ErrorStopped
	}

	w.Lock()
	w.inNums++
	w.Unlock()

	w.workChan <- fn
	return nil
}

// StopWait stops the worker pool and waits for all queued tasks tasks to complete.
func (w *WorkerPool) StopWait() {
	atomic.StoreUint32(&w.state, StateStopped)

	w.Lock()
	defer w.Unlock()
	w.taskDone.Wait()
}

func (w *WorkerPool) Restart() {
	atomic.StoreUint32(&w.state, StateRunning)
}

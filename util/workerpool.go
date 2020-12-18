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
	inNums     uint64
	outNums    uint64
	curWorkers int

	maxWorkers int
	workChan   chan func()

	taskDone *sync.Cond
	state    uint32
	sync.Mutex
}

// New creates and starts a pool of worker goroutines.
func NewWorkerPool(maxWorkers int, queueSize int) *WorkerPool {
	if maxWorkers <= 0 {
		panic("WorkerNum must be greater than zero")
	}

	w := &WorkerPool{
		maxWorkers: maxWorkers,
		workChan:   make(chan func(), queueSize),
	}

	w.taskDone = sync.NewCond(w)

	w.start()
	return w
}

var (
	// ErrorStopped when stopped
	ErrorStopped = errors.New("WorkerPool already stopped")
)

func (w *WorkerPool) wokerFunc() {
	w.Lock()
	w.curWorkers++
	w.Unlock()
LOOP:
	for fn := range w.workChan {
		fn()
		var needQuit bool
		w.Lock()
		w.outNums++
		if w.inNums == w.outNums {
			w.taskDone.Signal()
		}
		if w.curWorkers > w.maxWorkers {
			w.curWorkers--
			needQuit = true
		}
		w.Unlock()
		if needQuit {
			break LOOP
		}
	}
}

func (w *WorkerPool) start() {
	for i := 0; i < w.maxWorkers; i++ {
		go w.wokerFunc()
	}
}

// Resize ensures worker number match the expected one.
func (w *WorkerPool) Resize(maxWorkers int) {
	w.Lock()
	defer w.Unlock()
	for i := 0; i < maxWorkers-w.maxWorkers; i++ {
		go w.wokerFunc()
	}
	w.maxWorkers = maxWorkers
	// if maxWorkers<w.maxWorkers, redundant workers quit by themselves
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
	for w.inNums != w.outNums {
		w.taskDone.Wait()
	}
}

func (w *WorkerPool) Restart() {
	atomic.StoreUint32(&w.state, StateRunning)
}

package worker

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
)

var queueDepthLogEnabled = os.Getenv("TINYKV_QUEUE_DEPTH_LOG") == "1"

type TaskStop struct{}

type Task interface{}

type Worker struct {
	name     string
	sender   chan<- Task
	receiver <-chan Task
	closeCh  chan struct{}
	wg       *sync.WaitGroup
	fullWarn uint32
}

type TaskHandler interface {
	Handle(t Task)
}

type Starter interface {
	Start()
}

func (w *Worker) Start(handler TaskHandler) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer close(w.closeCh)
		if s, ok := handler.(Starter); ok {
			s.Start()
		}
		if queueDepthLogEnabled {
			go func() {
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						log.Infof("worker queue depth, name=%s len=%d cap=%d", w.name, len(w.receiver), cap(w.receiver))
					case <-w.closeCh:
						return
					}
				}
			}()
		}
		for {
			queueLen, queueCap := len(w.receiver), cap(w.receiver)
			if queueLen == queueCap {
				if atomic.CompareAndSwapUint32(&w.fullWarn, 0, 1) {
					log.Warnf("worker queue full, name=%s len=%d cap=%d", w.name, queueLen, queueCap)
				}
			} else if queueLen <= queueCap/2 {
				atomic.StoreUint32(&w.fullWarn, 0)
			}
			Task := <-w.receiver
			if _, ok := Task.(TaskStop); ok {
				return
			}
			handler.Handle(Task)
		}
	}()
}

func (w *Worker) Sender() chan<- Task {
	return w.sender
}

func (w *Worker) Stop() {
	w.sender <- TaskStop{}
}

const defaultWorkerCapacity = 128

func NewWorker(name string, wg *sync.WaitGroup) *Worker {
	return NewWorkerWithCapacity(name, wg, defaultWorkerCapacity)
}

func NewWorkerWithCapacity(name string, wg *sync.WaitGroup, capacity int) *Worker {
	ch := make(chan Task, capacity)
	return &Worker{
		sender:   (chan<- Task)(ch),
		receiver: (<-chan Task)(ch),
		name:     name,
		closeCh:  make(chan struct{}),
		wg:       wg,
	}
}

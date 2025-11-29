package workers

import (
	"log"
	"sync"
)

type Task struct {
	SessionID string
	Payload   any
}

type WorkerPool struct {
	NumWorkers int
	queues     map[int]chan Task
	wg         sync.WaitGroup
}

func NewWorkerPool(n int) *WorkerPool {
	wp := &WorkerPool{
		NumWorkers: n,
		queues:     make(map[int]chan Task),
	}

	for i := 0; i < n; i++ {
		ch := make(chan Task, 100)
		wp.queues[i] = ch

		wp.wg.Add(1)
		go func(id int, q chan Task) {
			defer wp.wg.Done()
			for task := range q {
				log.Printf("[worker-%d] Processing session=%s", id, task.SessionID)

				if handler, ok := task.Payload.(func()); ok {
					handler()
				}
			}
		}(i, ch)
	}

	return wp
}

func (wp *WorkerPool) Dispatch(sessionID string, fn func()) {
	workerID := int(HashString(sessionID)) % wp.NumWorkers
	wp.queues[workerID] <- Task{
		SessionID: sessionID,
		Payload:   fn,
	}
}

func (wp *WorkerPool) Stop() {
	for _, q := range wp.queues {
		close(q)
	}
	wp.wg.Wait()
}

func HashString(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

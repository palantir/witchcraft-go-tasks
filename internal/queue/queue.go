/*
Copyright 2015 The Kubernetes Authors.

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

package workqueue

import (
	"sync"
)

type Queue[T comparable] interface {
	Add(item T)
	Len() int
	Get() (item T, shutdown bool)
	ShutDown()
	ShutDownWithDrain()
	ShuttingDown() bool
}

func NewQueue[T comparable]() Queue[T] {
	return &Typed[T]{
		queue:  new(queue[T]),
		cond:   sync.NewCond(&sync.Mutex{}),
		stopCh: make(chan struct{}),
	}
}

type Typed[t comparable] struct {
	// queue defines the order in which we will work on items. Every
	// element of queue should be in the dirty set and not in the
	// processing set.
	queue *queue[t]

	cond *sync.Cond

	shuttingDown bool
	drain        bool

	// wg manages goroutines started by the queue to allow graceful shutdown
	// ShutDown() will wait for goroutines to exit before returning.
	wg sync.WaitGroup

	stopCh chan struct{}
	// stopOnce guarantees we only signal shutdown a single time
	stopOnce sync.Once
}

// Add marks item as needing processing. When the queue is shutdown new
// items will silently be ignored and not queued or marked as dirty for
// reprocessing.
func (q *Typed[T]) Add(item T) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return
	}
	q.queue.Push(item)
	q.cond.Signal()
}

// Len returns the current queue length, for informational purposes only. You
// shouldn't e.g. gate a call to Add() or Get() on Len() being a particular
// value, that can't be synchronized properly.
func (q *Typed[T]) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.queue.Len()
}

// Get blocks until it can return an item to be processed. If shutdown = true,
// the caller should end their goroutine. You must call Done with item when you
// have finished processing it.
func (q *Typed[T]) Get() (item T, shutdown bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for q.queue.Len() == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if q.queue.Len() == 0 {
		// We must be shutting down.
		return *new(T), true
	}

	item = q.queue.Pop()

	return item, false
}

// ShutDown will cause q to ignore all new items added to it. Worker
// goroutines will continue processing items in the queue until it is
// empty and then receive the shutdown signal.
func (q *Typed[T]) ShutDown() {
	defer q.wg.Wait()
	q.stopOnce.Do(func() {
		defer close(q.stopCh)
	})

	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.drain = false
	q.shuttingDown = true
	q.cond.Broadcast()
}

// ShutDownWithDrain is equivalent to ShutDown but waits until all items
// in the queue have been processed.
// ShutDown can be called after ShutDownWithDrain to force
// ShutDownWithDrain to stop waiting.
// Workers must call Done on an item after processing it, otherwise
// ShutDownWithDrain will block indefinitely.
func (q *Typed[T]) ShutDownWithDrain() {
	defer q.wg.Wait()
	q.stopOnce.Do(func() {
		defer close(q.stopCh)
	})
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.drain = true
	q.shuttingDown = true
	q.cond.Broadcast()
}

func (q *Typed[T]) ShuttingDown() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.shuttingDown
}

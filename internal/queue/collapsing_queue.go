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

	"github.com/palantir/witchcraft-go-tasks/internal/set"
)

type CollapsingQueue[T comparable] interface {
	Queue[T]
	Done(item T)
}

// NewCollapsingQueue  constructs a new work queue (see the package comment).
func NewCollapsingQueue[T comparable]() CollapsingQueue[T] {
	return &collapsingQueue[T]{
		queue:      new(queue[T]),
		dirty:      set.Set[T]{},
		processing: set.Set[T]{},
		cond:       sync.NewCond(&sync.Mutex{}),
		stopCh:     make(chan struct{}),
	}
}

type collapsingQueue[t comparable] struct {
	// queue defines the order in which we will work on items. Every
	// element of queue should be in the dirty set and not in the
	// processing set.
	queue *queue[t]

	// dirty defines all of the items that need to be processed.
	dirty set.Set[t]

	// Things that are currently being processed are in the processing set.
	// These things may be simultaneously in the dirty set. When we finish
	// processing something and remove it from this set, we'll check if
	// it's in the dirty set, and if so, add it to the queue.
	processing set.Set[t]

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
func (q *collapsingQueue[T]) Add(item T) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return
	}
	if q.dirty.Has(item) {
		// the same item is added again before it is processed, call the Touch
		// function if the queue cares about it (for e.g, reset its priority)
		if !q.processing.Has(item) {
			q.queue.Touch(item)
		}
		return
	}

	q.dirty.Insert(item)
	if q.processing.Has(item) {
		return
	}

	q.queue.Push(item)
	q.cond.Signal()
}

// Len returns the current queue length, for informational purposes only. You
// shouldn't e.g. gate a call to Add() or Get() on Len() being a particular
// value, that can't be synchronized properly.
func (q *collapsingQueue[T]) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.queue.Len()
}

// Get blocks until it can return an item to be processed. If shutdown = true,
// the caller should end their goroutine. You must call Done with item when you
// have finished processing it.
func (q *collapsingQueue[T]) Get() (item T, shutdown bool) {
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

	q.processing.Insert(item)
	q.dirty.Delete(item)

	return item, false
}

// Done marks item as done processing, and if it has been marked as dirty again
// while it was being processed, it will be re-added to the queue for
// re-processing.
func (q *collapsingQueue[T]) Done(item T) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.processing.Delete(item)
	if q.dirty.Has(item) {
		q.queue.Push(item)
		q.cond.Signal()
	} else if q.processing.Len() == 0 {
		q.cond.Signal()
	}
}

// ShutDown will cause q to ignore all new items added to it. Worker
// goroutines will continue processing items in the queue until it is
// empty and then receive the shutdown signal.
func (q *collapsingQueue[T]) ShutDown() {
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
func (q *collapsingQueue[T]) ShutDownWithDrain() {
	defer q.wg.Wait()
	q.stopOnce.Do(func() {
		defer close(q.stopCh)
	})
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.drain = true
	q.shuttingDown = true
	q.cond.Broadcast()

	for q.processing.Len() != 0 && q.drain {
		q.cond.Wait()
	}
}

func (q *collapsingQueue[T]) ShuttingDown() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	return q.shuttingDown
}

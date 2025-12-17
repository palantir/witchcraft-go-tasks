// Copyright (c) 2025 Palantir Technologies. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queue

import (
	"sync"

	"github.com/palantir/witchcraft-go-tasks/util/collections"
)

// CollapsingQueue is a thread-safe work queue that deduplicates items. If an item
// is added while it is already queued or being processed, the duplicate is collapsed
// into the existing entry. This is useful for scenarios where you want to ensure
// an item is processed, but don't need to process it multiple times if it's added
// repeatedly (e.g., reconciliation loops, cache invalidation).
//
// The type parameter T must be comparable to enable deduplication.
//
// Callers must call Done() after processing each item retrieved via Get(). If an
// item was re-added while being processed, Done() will re-queue it for processing.
type CollapsingQueue[T comparable] interface {
	Queue[T]
	// Done marks the item as finished processing. If the item was re-added to the
	// queue while it was being processed, it will be re-queued for another processing
	// cycle. Must be called exactly once for each successful Get() call.
	Done(item T)
}

// NewCollapsingQueue constructs a new deduplicating work queue.
func NewCollapsingQueue[T comparable]() CollapsingQueue[T] {
	return &collapsingQueue[T]{
		queue:      new(queueWrapper[T]),
		dirty:      collections.Set[T]{},
		processing: collections.Set[T]{},
		cond:       sync.NewCond(&sync.Mutex{}),
		stopCh:     make(chan struct{}),
	}
}

type collapsingQueue[T comparable] struct {
	// queue defines the order in which we will work on items. Every
	// element of queue should be in the dirty set and not in the
	// processing set.
	queue *queueWrapper[T]

	// dirty defines all of the items that need to be processed.
	dirty collections.Set[T]

	// Things that are currently being processed are in the processing set.
	// These things may be simultaneously in the dirty set. When we finish
	// processing something and remove it from this set, we'll check if
	// it's in the dirty set, and if so, add it to the queue.
	processing collections.Set[T]

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

func (q *collapsingQueue[T]) Add(item T) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return
	}
	if q.dirty.Has(item) {
		return
	}

	q.dirty.Insert(item)
	if q.processing.Has(item) {
		return
	}

	q.queue.Push(item)
	q.cond.Signal()
}

func (q *collapsingQueue[T]) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.queue.Len()
}

func (q *collapsingQueue[T]) Get() (item T, shutdown bool) {
	return q.GetWithCallback(nil)
}

func (q *collapsingQueue[T]) GetWithCallback(callback func()) (item T, shutdown bool) {
	q.cond.L.Lock()
	defer func() {
		if callback != nil {
			callback()
		}
		q.cond.L.Unlock()
	}()
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

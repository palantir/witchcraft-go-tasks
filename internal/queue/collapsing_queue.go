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
	"time"

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
	// Done marks the item as finished processing, and must be called once for each
	// item returned by Get or GetWithCallback. If the item was re-added to the
	// queue while it was being processed, it will be re-queued for another processing
	// cycle.
	Done(item T)

	// AddAfter adds an item to the queue after the provided delay. Delayed additions are collapsed
	// by item while waiting, with the earliest requested deadline taking precedence.
	AddAfter(item T, delay time.Duration)
	// AddRateLimited adds an item to the queue after the rate limiter determines
	// an appropriate delay. The delay increases with each requeue of the same item.
	// Use this when re-adding items that failed processing to implement backoff.
	AddRateLimited(item T)
	// ResetRateLimit resets the rate limiter's failure count for an item. Call this after
	// successful processing to clear the backoff state.
	ResetRateLimit(item T)
	// NumRequeues returns the number of times an item has been requeued via
	// AddRateLimited. Returns 0 if the item has never been requeued or was forgotten.
	NumRequeues(item T) int
}

// NewCollapsingQueue constructs a new deduplicating work queue.
func NewCollapsingQueue[T comparable]() CollapsingQueue[T] {
	return &collapsingQueue[T]{
		queue:          new(queueWrapper[T]),
		dirty:          collections.Set[T]{},
		processing:     collections.Set[T]{},
		cond:           sync.NewCond(&sync.Mutex{}),
		delayedEntries: make(map[T]*delayedEntry),
		rateLimiter:    NewItemExponentialFailureRateLimiter[T](time.Millisecond*500, time.Second*10),
	}
}

type delayedEntry struct {
	readyAt time.Time
	timer   *time.Timer
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

	// delayedLock serializes delayed entry creation with shutdown. This ensures that no timer can
	// be added after shutdown begins concurrently with the final wait.
	delayedLock    sync.Mutex
	delayedEntries map[T]*delayedEntry
	// wg manages delayed timer callbacks so shutdown can wait for callbacks already in progress.
	wg sync.WaitGroup

	rateLimiter RateLimiter[T]
}

func (q *collapsingQueue[T]) AddRateLimited(item T) {
	delay := q.rateLimiter.When(item)
	q.AddAfter(item, delay)
}

func (q *collapsingQueue[T]) AddAfter(item T, delay time.Duration) {
	if delay <= 0 {
		q.Add(item)
		return
	}

	readyAt := time.Now().Add(delay)
	q.delayedLock.Lock()
	defer q.delayedLock.Unlock()

	q.cond.L.Lock()
	shuttingDown := q.shuttingDown
	q.cond.L.Unlock()
	if shuttingDown {
		return
	}

	if existing, ok := q.delayedEntries[item]; ok {
		if !readyAt.Before(existing.readyAt) {
			return
		}
		if existing.timer.Stop() {
			q.wg.Done()
		}
	}

	entry := &delayedEntry{readyAt: readyAt}
	q.wg.Add(1)
	entry.timer = time.AfterFunc(delay, func() {
		defer q.wg.Done()

		q.delayedLock.Lock()
		if q.delayedEntries[item] != entry {
			q.delayedLock.Unlock()
			return
		}
		delete(q.delayedEntries, item)
		q.delayedLock.Unlock()

		q.Add(item)
	})
	q.delayedEntries[item] = entry
}

func (q *collapsingQueue[T]) ResetRateLimit(item T) {
	q.rateLimiter.ResetRateLimit(item)
}

func (q *collapsingQueue[T]) NumRequeues(item T) int {
	return q.rateLimiter.NumRequeues(item)
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
	defer q.cond.L.Unlock()
	for q.queue.Len() == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if q.queue.Len() == 0 {
		// We must be shutting down - don't call callback.
		return *new(T), true
	}

	item = q.queue.Pop()

	q.processing.Insert(item)
	q.dirty.Delete(item)

	if callback != nil {
		callback()
	}
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
	q.delayedLock.Lock()
	q.cond.L.Lock()
	q.drain = false
	q.shuttingDown = true
	q.cond.Broadcast()
	q.cond.L.Unlock()
	q.cancelDelayedEntries()
	q.delayedLock.Unlock()
	q.wg.Wait()
}

func (q *collapsingQueue[T]) ShutDownWithDrain() {
	q.delayedLock.Lock()
	q.cond.L.Lock()
	q.drain = true
	q.shuttingDown = true
	q.cond.Broadcast()
	q.cond.L.Unlock()
	q.cancelDelayedEntries()
	q.delayedLock.Unlock()
	q.wg.Wait()

	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for q.processing.Len() != 0 && q.drain {
		q.cond.Wait()
	}
}

// cancelDelayedEntries must be called while delayedLock is held.
func (q *collapsingQueue[T]) cancelDelayedEntries() {
	for item, entry := range q.delayedEntries {
		delete(q.delayedEntries, item)
		if entry.timer.Stop() {
			q.wg.Done()
		}
	}
}

func (q *collapsingQueue[T]) ShuttingDown() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	return q.shuttingDown
}

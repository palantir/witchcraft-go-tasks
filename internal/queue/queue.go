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

import "sync/atomic"

// Queue is a generic work queue that provides blocking Get operations
// and graceful shutdown capabilities. Items are processed in FIFO order.
type Queue[T any] interface {
	Add(item T)
	Len() int
	Get() (item T, shutdown bool)
	GetWithCallback(callback func()) (item T, shutdown bool)
	ShutDown()
	ShutDownWithDrain()
	ShuttingDown() bool
}

// NewQueue constructs a new work queue. Unlike CollapsingQueue, this queue
// does not deduplicate items - each Add results in a corresponding Get.
func NewQueue[T any]() Queue[T] {
	return &queue[T]{
		delegate: NewCollapsingQueue[queueItem](),
	}
}

// queueItem wraps an item with a unique ID to prevent collapsing behavior.
// The item is stored as any since the user's T doesn't need to be comparable.
type queueItem struct {
	id   uint64
	item any
}

// queue wraps CollapsingQueue with unique item IDs to prevent deduplication.
type queue[T any] struct {
	delegate CollapsingQueue[queueItem]
	nextID   atomic.Uint64
}

// Add marks item as needing processing. When the queue is shutdown new
// items will silently be ignored.
func (q *queue[T]) Add(item T) {
	wrapped := queueItem{
		id:   q.nextID.Add(1),
		item: item,
	}
	q.delegate.Add(wrapped)
}

// Len returns the current queue length, for informational purposes only. You
// shouldn't e.g. gate a call to Add() or Get() on Len() being a particular
// value, that can't be synchronized properly.
func (q *queue[T]) Len() int {
	return q.delegate.Len()
}

// Get blocks until it can return an item to be processed. If shutdown = true,
// the caller should end their goroutine.
func (q *queue[T]) Get() (item T, shutdown bool) {
	return q.GetWithCallback(nil)
}

// GetWithCallback blocks until it can return an item to be processed. The callback
// is invoked while holding the queue lock, before returning. If shutdown = true,
// the caller should end their goroutine.
func (q *queue[T]) GetWithCallback(callback func()) (item T, shutdown bool) {
	wrapped, shutdown := q.delegate.GetWithCallback(callback)
	if shutdown {
		return *new(T), true
	}
	// Immediately mark as done since Queue doesn't track processing state
	q.delegate.Done(wrapped)
	return wrapped.item.(T), false
}

// ShutDown will cause q to ignore all new items added to it. Worker
// goroutines blocked on Get will be unblocked and receive shutdown = true.
func (q *queue[T]) ShutDown() {
	q.delegate.ShutDown()
}

// ShutDownWithDrain is equivalent to ShutDown but waits until all items
// in the queue have been retrieved.
func (q *queue[T]) ShutDownWithDrain() {
	q.delegate.ShutDownWithDrain()
}

// ShuttingDown returns true if the queue is shutting down.
func (q *queue[T]) ShuttingDown() bool {
	return q.delegate.ShuttingDown()
}

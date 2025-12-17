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

// Queue is a thread-safe generic work queue that provides blocking Get operations
// and graceful shutdown capabilities. Items are processed in FIFO order.
//
// Unlike CollapsingQueue, this queue does not deduplicate items - every call to Add() adds an element that can be retrieved
// by a corresponding Get(). This is suitable for task queues where every submitted
// item must be processed exactly once.
//
// The type parameter T can be any type since no comparison is needed.
type Queue[T any] interface {
	// Add enqueues an item for processing. If the queue is shutting down,
	// the item is silently ignored.
	Add(item T)
	// Len returns the current number of items waiting to be processed in the queue.
	// This is informational only and should not be used for synchronization decisions.
	Len() int
	// Get blocks until an item is available and returns it. If shutdown is true,
	// the queue has been shut down: in this case, the returned item is the zero
	// value of T and should be ignored, and the caller should exit their processing loop.
	Get() (item T, shutdown bool)
	// GetWithCallback is like Get but invokes the callback while holding the queue
	// lock, before returning. This can be used to perform atomic operations when
	// an item is dequeued.
	GetWithCallback(callback func()) (item T, shutdown bool)
	// ShutDown initiates shutdown and returns (does not block).
	// Once this function is called, new items cannot be added
	// (Add() becomes a noop). Any elements in the queue that
	// were present before it started shutting down will still be returned
	// by Get() calls with shutdown=false. Once the queue is empty,
	// Get() calls will return with shutdown=true.
	ShutDown()
	// ShutDownWithDrain initiates shutdown and blocks until all queued items have
	// been retrieved via Get(). Can be interrupted by calling ShutDown().
	ShutDownWithDrain()
	// ShuttingDown returns true if ShutDown() or ShutDownWithDrain() has been called.
	ShuttingDown() bool
}

// NewQueue constructs a new work queue. Unlike CollapsingQueue, this queue
// does not deduplicate items - each Add results in a corresponding Get.
func NewQueue[T any]() Queue[T] {
	return &queue[T]{
		delegate: NewCollapsingQueue[*item[T]](),
	}
}

// item wraps a value. Under the hood, the queue stores *item[T] elements.
// Because pointers are always comparable (by address), *item[T] satisfies comparable
// even when T doesn't. Each Add creates a new pointer, ensuring uniqueness
// (which also prevents collapsing elements).
type item[T any] struct {
	value T
}

// queue delegates to CollapsingQueue using pointer-wrapped items to prevent deduplication.
type queue[T any] struct {
	delegate CollapsingQueue[*item[T]]
}

func (q *queue[T]) Add(val T) {
	// Each Add creates a new pointer - always unique, never collapses
	q.delegate.Add(&item[T]{value: val})
}

func (q *queue[T]) Len() int {
	return q.delegate.Len()
}

func (q *queue[T]) Get() (T, bool) {
	return q.GetWithCallback(nil)
}

func (q *queue[T]) GetWithCallback(callback func()) (T, bool) {
	wrapped, shutdown := q.delegate.GetWithCallback(callback)
	if shutdown {
		return *new(T), true
	}
	// Auto-mark done since Queue doesn't track processing state
	q.delegate.Done(wrapped)
	return wrapped.value, false
}

func (q *queue[T]) ShutDown() {
	q.delegate.ShutDown()
}

func (q *queue[T]) ShutDownWithDrain() {
	q.delegate.ShutDownWithDrain()
}

func (q *queue[T]) ShuttingDown() bool {
	return q.delegate.ShuttingDown()
}

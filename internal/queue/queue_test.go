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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue_AddAndGetNonComparable(t *testing.T) {
	type MyFunc func() string
	q2 := NewQueue[MyFunc]()
	q2.Add(func() string { return "hello, world" })
	q2.Add(func() string { return "goodbye, world" })
}

func TestQueue_AddAndGet(t *testing.T) {
	q := NewQueue[string]()
	q.Add("item1")
	q.Add("item2")
	assert.Equal(t, 2, q.Len())
	item, shutdown := q.Get()
	assert.False(t, shutdown)
	assert.Equal(t, "item1", item)
	item, shutdown = q.Get()
	assert.False(t, shutdown)
	assert.Equal(t, "item2", item)
	assert.Equal(t, 0, q.Len())
}

func TestQueue_GetWithCallback(t *testing.T) {
	q := NewQueue[string]()
	q.Add("item1")
	var callbackCalled bool
	item, shutdown := q.GetWithCallback(func() {
		callbackCalled = true
	})
	assert.False(t, shutdown)
	assert.Equal(t, "item1", item)
	assert.True(t, callbackCalled)
}

func TestQueue_GetWithNilCallback(t *testing.T) {
	q := NewQueue[string]()
	q.Add("item1")
	item, shutdown := q.GetWithCallback(nil)
	assert.False(t, shutdown)
	assert.Equal(t, "item1", item)
}

func TestQueue_GetBlocksUntilItemAdded(t *testing.T) {
	q := NewQueue[string]()
	var item string
	var shutdown bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		item, shutdown = q.Get()
	}()
	q.Add("item1")
	wg.Wait()
	assert.False(t, shutdown)
	assert.Equal(t, "item1", item)
}

func TestQueue_ShutDown(t *testing.T) {
	q := NewQueue[string]()
	assert.False(t, q.ShuttingDown())
	q.ShutDown()
	assert.True(t, q.ShuttingDown())
}

func TestQueue_ShutDownUnblocksGet(t *testing.T) {
	q := NewQueue[string]()
	var item string
	var shutdown bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		item, shutdown = q.Get()
	}()
	q.ShutDown()
	wg.Wait()
	assert.True(t, shutdown)
	assert.Equal(t, "", item)
}

func TestQueue_AddIgnoredAfterShutdown(t *testing.T) {
	q := NewQueue[string]()
	q.ShutDown()
	q.Add("item1")
	assert.Equal(t, 0, q.Len())
}

func TestQueue_ShutDownWithDrain(t *testing.T) {
	q := NewQueue[string]()
	q.Add("item1")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		q.ShutDownWithDrain()
	}()
	item, shutdown := q.Get()
	assert.False(t, shutdown)
	assert.Equal(t, "item1", item)
	wg.Wait()
	assert.True(t, q.ShuttingDown())
}

func TestQueue_MultipleShutDownCalls(t *testing.T) {
	q := NewQueue[string]()
	q.ShutDown()
	q.ShutDown()
	assert.True(t, q.ShuttingDown())
}

func TestQueue_ConcurrentAddAndGet(t *testing.T) {
	q := NewQueue[int]()
	var wg sync.WaitGroup
	numItems := 100
	var received atomic.Int32
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range numItems {
			q.Add(i)
		}
	}()
	go func() {
		defer wg.Done()
		for range numItems {
			_, shutdown := q.Get()
			if !shutdown {
				received.Add(1)
			}
		}
	}()
	wg.Wait()
	assert.Equal(t, int32(numItems), received.Load())
}

func TestQueue_MultipleProducersMultipleConsumers(t *testing.T) {
	q := NewQueue[int]()
	numProducers := 10
	numConsumers := 10
	itemsPerProducer := 100
	totalItems := numProducers * itemsPerProducer
	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup
	var received atomic.Int32
	for range numProducers {
		producerWg.Add(1)
		go func() {
			defer producerWg.Done()
			for i := range itemsPerProducer {
				q.Add(i)
			}
		}()
	}
	for range numConsumers {
		consumerWg.Add(1)
		go func() {
			defer consumerWg.Done()
			for {
				_, shutdown := q.Get()
				if shutdown {
					return
				}
				received.Add(1)
			}
		}()
	}
	producerWg.Wait()
	q.ShutDown()
	consumerWg.Wait()
	assert.Equal(t, int32(totalItems), received.Load())
}

func TestQueue_ShutDownWithMultipleBlockedGetters(t *testing.T) {
	q := NewQueue[string]()
	numGetters := 10
	var wg sync.WaitGroup
	var shutdownCount atomic.Int32
	for range numGetters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, shutdown := q.Get()
			if shutdown {
				shutdownCount.Add(1)
			}
		}()
	}
	q.ShutDown()
	wg.Wait()
	assert.Equal(t, int32(numGetters), shutdownCount.Load())
}

func TestQueue_ConcurrentAddAndShutDown(t *testing.T) {
	q := NewQueue[int]()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 1000 {
			q.Add(i)
		}
	}()
	go func() {
		defer wg.Done()
		q.ShutDown()
	}()
	wg.Wait()
	assert.True(t, q.ShuttingDown())
}

func TestQueue_ConcurrentLenCalls(t *testing.T) {
	q := NewQueue[int]()
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				q.Add(1)
				_ = q.Len()
			}
		}()
	}
	wg.Wait()
}

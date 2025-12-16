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
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	testTimeout  = 1 * time.Second
	testInterval = 10 * time.Millisecond
)

func TestCollapsingQueue_AddAndGet(t *testing.T) {
	q := NewCollapsingQueue[string]()
	q.Add("item1")
	q.Add("item2")
	assert.Equal(t, 2, q.Len())
	item, shutdown := q.Get()
	assert.False(t, shutdown)
	assert.Equal(t, "item1", item)
	q.Done(item)
	item, shutdown = q.Get()
	assert.False(t, shutdown)
	assert.Equal(t, "item2", item)
	q.Done(item)
	assert.Equal(t, 0, q.Len())
}

func TestCollapsingQueue_DuplicateAddCollapsed(t *testing.T) {
	q := NewCollapsingQueue[string]()
	q.Add("item1")
	q.Add("item1")
	q.Add("item1")
	assert.Equal(t, 1, q.Len())
	item, shutdown := q.Get()
	assert.False(t, shutdown)
	assert.Equal(t, "item1", item)
	q.Done(item)
	assert.Equal(t, 0, q.Len())
}

func TestCollapsingQueue_ReAddWhileProcessing(t *testing.T) {
	q := NewCollapsingQueue[string]()
	q.Add("item1")
	item, shutdown := q.Get()
	assert.False(t, shutdown)
	assert.Equal(t, "item1", item)
	assert.Equal(t, 0, q.Len())
	q.Add("item1")
	assert.Equal(t, 0, q.Len())
	q.Done(item)
	assert.Equal(t, 1, q.Len())
	item, shutdown = q.Get()
	assert.False(t, shutdown)
	assert.Equal(t, "item1", item)
	q.Done(item)
	assert.Equal(t, 0, q.Len())
}

func TestCollapsingQueue_DuplicateAddWhileNotProcessing(t *testing.T) {
	q := NewCollapsingQueue[string]()
	q.Add("item1")
	q.Add("item1")
	assert.Equal(t, 1, q.Len())
}

func TestCollapsingQueue_GetWithCallback(t *testing.T) {
	q := NewCollapsingQueue[string]()
	q.Add("item1")
	var callbackCalled bool
	item, shutdown := q.GetWithCallback(func() {
		callbackCalled = true
	})
	assert.False(t, shutdown)
	assert.Equal(t, "item1", item)
	assert.True(t, callbackCalled)
	q.Done(item)
}

func TestCollapsingQueue_GetWithNilCallback(t *testing.T) {
	q := NewCollapsingQueue[string]()
	q.Add("item1")
	item, shutdown := q.GetWithCallback(nil)
	assert.False(t, shutdown)
	assert.Equal(t, "item1", item)
	q.Done(item)
}

func TestCollapsingQueue_GetBlocksUntilItemAdded(t *testing.T) {
	q := NewCollapsingQueue[string]()
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

func TestCollapsingQueue_ShutDown(t *testing.T) {
	q := NewCollapsingQueue[string]()
	assert.False(t, q.ShuttingDown())
	q.ShutDown()
	assert.True(t, q.ShuttingDown())
}

func TestCollapsingQueue_ShutDownUnblocksGet(t *testing.T) {
	q := NewCollapsingQueue[string]()
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

func TestCollapsingQueue_AddIgnoredAfterShutdown(t *testing.T) {
	q := NewCollapsingQueue[string]()
	q.ShutDown()
	q.Add("item1")
	assert.Equal(t, 0, q.Len())
}

func TestCollapsingQueue_ShutDownWithDrain(t *testing.T) {
	q := NewCollapsingQueue[string]()
	q.Add("item1")
	var drainDone atomic.Bool
	go func() {
		q.ShutDownWithDrain()
		drainDone.Store(true)
	}()
	item, shutdown := q.Get()
	assert.False(t, shutdown)
	assert.Equal(t, "item1", item)
	assert.False(t, drainDone.Load())
	q.Done(item)
	assert.Eventually(t, func() bool {
		return drainDone.Load()
	}, testTimeout, testInterval)
	assert.True(t, q.ShuttingDown())
}

func TestCollapsingQueue_ShutDownWithDrainInterruptedByShutDown(t *testing.T) {
	q := NewCollapsingQueue[string]()
	q.Add("item1")
	item, _ := q.Get()
	var drainDone atomic.Bool
	go func() {
		q.ShutDownWithDrain()
		drainDone.Store(true)
	}()
	// Give the goroutine time to start and block in ShutDownWithDrain
	time.Sleep(50 * time.Millisecond)
	q.ShutDown()
	assert.Eventually(t, func() bool {
		return drainDone.Load()
	}, testTimeout, testInterval)
	q.Done(item)
}

func TestCollapsingQueue_MultipleShutDownCalls(t *testing.T) {
	q := NewCollapsingQueue[string]()
	q.ShutDown()
	q.ShutDown()
	assert.True(t, q.ShuttingDown())
}

func TestCollapsingQueue_DoneSignalsWhenProcessingEmpty(t *testing.T) {
	q := NewCollapsingQueue[string]()
	q.Add("item1")
	item, _ := q.Get()
	q.Done(item)
	assert.Equal(t, 0, q.Len())
}

func TestCollapsingQueue_ConcurrentAddAndGet(t *testing.T) {
	q := NewCollapsingQueue[int]()
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
			item, shutdown := q.Get()
			if !shutdown {
				received.Add(1)
				q.Done(item)
			}
		}
	}()
	wg.Wait()
	assert.Equal(t, int32(numItems), received.Load())
}

func TestCollapsingQueue_ConcurrentReAddWhileProcessing(t *testing.T) {
	q := NewCollapsingQueue[int]()
	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup
	numWorkers := 5
	numIterations := 100
	var processed atomic.Int32
	for range numWorkers {
		producerWg.Add(1)
		go func() {
			defer producerWg.Done()
			for range numIterations {
				q.Add(1)
			}
		}()
	}
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for {
			item, shutdown := q.Get()
			if shutdown {
				return
			}
			processed.Add(1)
			q.Done(item)
		}
	}()
	producerWg.Wait()
	q.ShutDown()
	consumerWg.Wait()
	assert.GreaterOrEqual(t, processed.Load(), int32(1))
}

func TestCollapsingQueue_MultipleWorkersDone(t *testing.T) {
	q := NewCollapsingQueue[int]()
	numItems := 100
	for i := range numItems {
		q.Add(i)
	}
	var wg sync.WaitGroup
	var processed atomic.Int32
	numWorkers := 10
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				item, shutdown := q.Get()
				if shutdown {
					return
				}
				processed.Add(1)
				q.Done(item)
			}
		}()
	}
	assert.Eventually(t, func() bool {
		return processed.Load() >= int32(numItems)
	}, testTimeout, testInterval)
	q.ShutDown()
	wg.Wait()
	assert.Equal(t, int32(numItems), processed.Load())
}

func TestCollapsingQueue_ShutDownWithMultipleBlockedGetters(t *testing.T) {
	q := NewCollapsingQueue[string]()
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

func TestCollapsingQueue_ConcurrentAddAndShutDown(t *testing.T) {
	q := NewCollapsingQueue[int]()
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

func TestCollapsingQueue_ShutDownWithDrainMultipleWorkers(t *testing.T) {
	q := NewCollapsingQueue[int]()
	numItems := 50
	for i := range numItems {
		q.Add(i)
	}
	var wg sync.WaitGroup
	var processed atomic.Int32
	numWorkers := 5
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				item, shutdown := q.Get()
				if shutdown {
					return
				}
				processed.Add(1)
				q.Done(item)
			}
		}()
	}
	q.ShutDownWithDrain()
	wg.Wait()
	assert.Equal(t, int32(numItems), processed.Load())
}

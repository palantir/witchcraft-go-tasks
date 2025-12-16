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

func TestCollapsingQueue_TouchCalledOnDuplicateNotProcessing(t *testing.T) {
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
	var drainStarted atomic.Bool
	var drainDone atomic.Bool
	go func() {
		drainStarted.Store(true)
		q.ShutDownWithDrain()
		drainDone.Store(true)
	}()
	assert.Eventually(t, func() bool {
		return drainStarted.Load()
	}, testTimeout, testInterval)
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
		for i := 0; i < numItems; i++ {
			q.Add(i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < numItems; i++ {
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

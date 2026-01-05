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

package executor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palantir/witchcraft-go-health/v2/conjure/witchcraft/api/health"
	"github.com/palantir/witchcraft-go-health/v2/sources/window"
	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/internal/testcontext"
	"github.com/palantir/witchcraft-go-tasks/workerpool"
	"github.com/stretchr/testify/assert"
)

type testItem string

func (t testItem) String() string {
	return string(t)
}

func TestNewDefaultItemSubmitter(t *testing.T) {
	ctx := testcontext.GetTestContext(t)
	healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
	consumer := function.NewConsumerFromFunc(func(ctx context.Context, item testItem) error {
		return nil
	})
	pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
	submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource)
	assert.NotNil(t, submitter)
}

func TestItemSubmitter_Submit(t *testing.T) {
	t.Run("processes submitted item", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		var processed atomic.Value
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, item testItem) error {
			processed.Store(item)
			return nil
		})
		pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
		submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource)
		submitter.Submit(ctx, testItem("test-item"))
		assert.Eventually(t, func() bool {
			v := processed.Load()
			return v != nil && v.(testItem) == testItem("test-item")
		}, time.Second, 10*time.Millisecond)
	})
	t.Run("requeues item on failure", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		var attempts atomic.Int32
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, item testItem) error {
			if attempts.Add(1) <= 1 {
				return errors.New("transient error")
			}
			return nil
		})
		pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
		submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource)
		submitter.Submit(ctx, testItem("test-item"))
		assert.Eventually(t, func() bool {
			return attempts.Load() >= 2
		}, 5*time.Second, 10*time.Millisecond)
	})
	t.Run("stops requeuing after max requeues", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		var attempts atomic.Int32
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, item testItem) error {
			attempts.Add(1)
			return errors.New("permanent error")
		})
		pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
		maxRequeues := 2
		submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource,
			WithMaxNumRequeues[testItem](maxRequeues),
			WithErrorLogger[testItem](func(ctx context.Context, err error) {}),
		)
		submitter.Submit(ctx, testItem("test-item"))
		assert.Eventually(t, func() bool {
			return attempts.Load() >= int32(maxRequeues+1)
		}, 10*time.Second, 10*time.Millisecond)
		attemptsAfterMax := attempts.Load()
		assert.Equal(t, attemptsAfterMax, attempts.Load(), "should not process after max requeues")
	})
	t.Run("submits health check on success", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		var processed atomic.Bool
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, item testItem) error {
			processed.Store(true)
			return nil
		})
		pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
		submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource)
		submitter.Submit(ctx, testItem("test-item"))
		assert.Eventually(t, func() bool {
			return processed.Load()
		}, time.Second, 10*time.Millisecond)
		assert.Eventually(t, func() bool {
			status := healthCheckSource.HealthStatus(ctx)
			check, ok := status.Checks[health.CheckType("test")]
			return ok && check.State.Value() == health.HealthState_HEALTHY
		}, time.Second, 10*time.Millisecond)
	})
	t.Run("submits health check on failure", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		var processed atomic.Bool
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, item testItem) error {
			processed.Store(true)
			return errors.New("processing error")
		})
		pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
		submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource,
			WithMaxNumRequeues[testItem](0),
			WithErrorLogger[testItem](func(ctx context.Context, err error) {}),
		)
		submitter.Submit(ctx, testItem("test-item"))
		assert.Eventually(t, func() bool {
			return processed.Load()
		}, time.Second, 10*time.Millisecond)
		assert.Eventually(t, func() bool {
			status := healthCheckSource.HealthStatus(ctx)
			check, ok := status.Checks[health.CheckType("test")]
			return ok && check.State.Value() == health.HealthState_ERROR
		}, time.Second, 10*time.Millisecond)
	})
	t.Run("calls custom error logger", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		expectedErr := errors.New("custom error")
		var loggedErr atomic.Value
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, item testItem) error {
			return expectedErr
		})
		pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
		submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource,
			WithMaxNumRequeues[testItem](0),
			WithErrorLogger[testItem](func(ctx context.Context, err error) {
				loggedErr.Store(err)
			}),
		)
		submitter.Submit(ctx, testItem("test-item"))
		assert.Eventually(t, func() bool {
			v := loggedErr.Load()
			return v != nil && v.(error) == expectedErr
		}, time.Second, 10*time.Millisecond)
	})
	t.Run("collapses duplicate submissions", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		var processCount atomic.Int32
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, item testItem) error {
			processCount.Add(1)
			return nil
		})
		pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
		submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource)
		for i := 0; i < 10; i++ {
			submitter.Submit(ctx, testItem("same-item"))
		}
		assert.Eventually(t, func() bool {
			return processCount.Load() >= 1
		}, time.Second, 10*time.Millisecond)
		assert.Less(t, processCount.Load(), int32(10), "duplicates should be collapsed")
	})
	t.Run("handles concurrent submissions from multiple goroutines", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		var processedItems sync.Map
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, item testItem) error {
			processedItems.Store(item, true)
			return nil
		})
		pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
		submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource)
		numGoroutines := 10
		numItemsPerGoroutine := 10
		var wg sync.WaitGroup
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < numItemsPerGoroutine; i++ {
					item := testItem(fmt.Sprintf("item-%d-%d", goroutineID, i))
					submitter.Submit(ctx, item)
				}
			}(g)
		}
		wg.Wait()
		expectedItems := numGoroutines * numItemsPerGoroutine
		assert.Eventually(t, func() bool {
			count := 0
			processedItems.Range(func(_, _ any) bool {
				count++
				return true
			})
			return count == expectedItems
		}, 5*time.Second, 10*time.Millisecond)
	})
}

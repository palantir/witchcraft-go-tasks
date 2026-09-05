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
	"sync/atomic"
	"testing"
	"time"

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-health/v2/conjure/witchcraft/api/health"
	"github.com/palantir/witchcraft-go-health/v2/sources/window"
	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/internal/queue"
	"github.com/palantir/witchcraft-go-tasks/internal/testcontext"
	"github.com/palantir/witchcraft-go-tasks/workerpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testItem string

func (t testItem) String() string {
	return string(t)
}

func metricHasTag(registry metrics.RootRegistry, metricName, tagKey, tagValue string) bool {
	found := false
	registry.Each(func(name string, tags metrics.Tags, _ metrics.MetricVal) {
		if name != metricName {
			return
		}
		for _, tag := range tags {
			if tag.Key() == tagKey && tag.Value() == tagValue {
				found = true
			}
		}
	})
	return found
}

func metricHasTagKey(registry metrics.RootRegistry, metricName, tagKey string) bool {
	found := false
	registry.Each(func(name string, tags metrics.Tags, _ metrics.MetricVal) {
		if name != metricName {
			return
		}
		for _, tag := range tags {
			if tag.Key() == tagKey {
				found = true
			}
		}
	})
	return found
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
			WithMaxNumRequeues(maxRequeues),
			WithErrorLogger(func(ctx context.Context, err error) {}),
		)
		submitter.Submit(ctx, testItem("test-item"))
		assert.Eventually(t, func() bool {
			return attempts.Load() == int32(maxRequeues+1)
		}, 10*time.Second, 10*time.Millisecond)
	})
	t.Run("submits health check on success", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, item testItem) error {
			return nil
		})
		pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
		submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource)
		submitter.Submit(ctx, testItem("test-item"))
		assert.Eventually(t, func() bool {
			status := healthCheckSource.HealthStatus(ctx)
			check, ok := status.Checks[health.CheckType("test")]
			return ok && check.State.Value() == health.HealthState_HEALTHY
		}, time.Second, 10*time.Millisecond)
	})
	t.Run("submits health check on failure", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, item testItem) error {
			return errors.New("processing error")
		})
		pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
		submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource,
			WithMaxNumRequeues(0),
			WithErrorLogger(func(ctx context.Context, err error) {}),
		)
		submitter.Submit(ctx, testItem("test-item"))
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
			WithMaxNumRequeues(0),
			WithErrorLogger(func(ctx context.Context, err error) {
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
		for range 10 {
			submitter.Submit(ctx, testItem("same-item"))
		}
		assert.Eventually(t, func() bool {
			return processCount.Load() >= 1
		}, time.Second, 10*time.Millisecond)
		assert.Less(t, processCount.Load(), int32(10), "duplicates should be collapsed")
	})
	t.Run("applies itemsubmittername tag when set", func(t *testing.T) {
		registry := metrics.NewRootMetricsRegistry()
		ctx := metrics.WithRegistry(testcontext.GetTestContext(t), registry)
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		var processed atomic.Bool
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, item testItem) error {
			processed.Store(true)
			return nil
		})
		pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
		submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource, WithItemSubmitterName("test-pool"))
		submitter.Submit(ctx, testItem("test-item"))
		assert.Eventually(t, func() bool {
			if !processed.Load() {
				return false
			}
			return metricHasTag(registry, "com.palantir.witchcraft.process_element_duration", "itemsubmittername", "test-pool")
		}, time.Second, 10*time.Millisecond)
	})
	t.Run("does not apply itemsubmittername tag when unset", func(t *testing.T) {
		registry := metrics.NewRootMetricsRegistry()
		ctx := metrics.WithRegistry(testcontext.GetTestContext(t), registry)
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		var processed atomic.Bool
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, item testItem) error {
			processed.Store(true)
			return nil
		})
		pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
		submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource)
		submitter.Submit(ctx, testItem("test-item"))
		assert.Eventually(t, processed.Load, time.Second, 10*time.Millisecond)
		assert.False(t, metricHasTagKey(registry, "com.palantir.witchcraft.process_element_duration", "itemsubmittername"),
			"should not have itemsubmittername tag when option is unset")
	})
}

func TestItemSubmitter_SubmitAfter(t *testing.T) {
	ctx := testcontext.GetTestContext(t)
	healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
	processed := make(chan testItem, 1)
	consumer := function.NewConsumerFromFunc(func(_ context.Context, item testItem) error {
		processed <- item
		return nil
	})
	pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
	submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource)

	submitter.SubmitAfter(ctx, testItem("test-item"), 20*time.Millisecond)

	select {
	case item := <-processed:
		assert.Equal(t, testItem("test-item"), item)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delayed item to be processed")
	}
}

func TestItemSubmitter_ShutsDownQueueWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(testcontext.GetTestContext(t))
	healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
	consumer := function.NewConsumerFromFunc(func(_ context.Context, _ testItem) error { return nil })
	pool := workerpool.NewDefaultConsumerWorkerPool(ctx, consumer)
	submitter := NewDefaultItemSubmitter(ctx, pool, healthCheckSource)
	concreteSubmitter, ok := submitter.(*defaultItemSubmitter[testItem])
	require.True(t, ok)
	submitter.SubmitAfter(ctx, testItem("test-item"), time.Hour)

	cancel()

	assert.Eventually(t, concreteSubmitter.queue.ShuttingDown, time.Second, 10*time.Millisecond)
}

func TestItemSubmitterUnlimitedRetriesRequeuesAfterMaximum(t *testing.T) {
	ctx := testcontext.GetTestContext(t)
	itemQueue := queue.NewCollapsingQueue[testItem]()
	defer itemQueue.ShutDown()
	submitter := defaultItemSubmitter[testItem]{
		queue: itemQueue,
		config: ItemSubmitterConfig{
			maxNumRequeues:   0,
			unlimitedRetries: true,
			logError:         func(context.Context, error) {},
		},
	}

	submitter.handleProcessError(ctx, testItem("test-item"), errors.New("persistent error"))

	assert.Eventually(t, func() bool {
		return itemQueue.Len() == 1
	}, time.Second, 10*time.Millisecond)
}

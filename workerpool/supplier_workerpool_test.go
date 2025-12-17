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

package workerpool

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/util/async"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_SerialWorkerpoolFetching(t *testing.T) {
	workerPool := NewDefaultSupplierWorkerPool[string](context.Background())
	workerPoolTyped := workerPool.(*defaultSupplierWorkerPool[string])
	simpleGet := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "a", nil
	})
	// Before doing anything
	assert.Equal(t, 0, workerPoolTyped.queue.Len())
	assert.Equal(t, 0, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, 0, int(workerPoolTyped.totalCount.Load()))
	// Single submit
	f1 := workerPool.Submit(context.Background(), simpleGet)
	result, err := f1.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
	assert.Equal(t, 0, workerPoolTyped.queue.Len())
	assert.Equal(t, 1, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, 1, int(workerPoolTyped.totalCount.Load()))
	// And another submit
	f1 = workerPool.Submit(context.Background(), simpleGet)
	result, err = f1.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
	assert.Equal(t, 0, workerPoolTyped.queue.Len())
	assert.Equal(t, 1, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, 1, int(workerPoolTyped.totalCount.Load()))
	// Ensure callback works as well
	seen := false
	workerPool.SubmitWithCallback(context.Background(), simpleGet, func(ctx context.Context, s string, err error) {
		assert.NoError(t, err)
		assert.Equal(t, "a", s)
		seen = true
	})
	assert.Eventually(t, func() bool {
		return seen
	}, time.Millisecond*100, time.Millisecond*10)
}

func Test_SerialWorkerpoolFetching_WorkerCap(t *testing.T) {
	start := time.Now()
	workerPool := NewDefaultSupplierWorkerPool[string](context.Background(), WithMaxNumberOfThreads(2))
	workerPoolTyped := workerPool.(*defaultSupplierWorkerPool[string])
	simpleGet := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		time.Sleep(time.Millisecond * 500)
		return "a", nil
	})
	// Single submit
	f1 := workerPool.Submit(context.Background(), simpleGet)
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 0, workerPoolTyped.queue.Len())
	assert.Equal(t, 0, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, 1, int(workerPoolTyped.totalCount.Load()))

	f2 := workerPool.Submit(context.Background(), simpleGet)
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 0, workerPoolTyped.queue.Len())
	assert.Equal(t, 0, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, 2, int(workerPoolTyped.totalCount.Load()))
	f3 := workerPool.Submit(context.Background(), simpleGet)
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 1, workerPoolTyped.queue.Len())
	assert.Equal(t, 0, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, 2, int(workerPoolTyped.totalCount.Load()))
	result, err := f1.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
	result, err = f2.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
	timeInRange(t, start, 999, 500)
	result, err = f3.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
	time.Sleep(time.Millisecond * 10)
	timeInRange(t, start, 1499, 1001)
	assert.Equal(t, 0, workerPoolTyped.queue.Len())
	assert.Equal(t, 2, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, 2, int(workerPoolTyped.totalCount.Load()))
}

func Test_WorkerpoolFetching_NoCapThroughput(t *testing.T) {
	start := time.Now()
	workerPool := NewDefaultSupplierWorkerPool[string](context.Background())
	simpleGet := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "a", nil
	})
	var toHold []async.Future[string]
	for i := 1; i <= 10000; i++ {
		toHold = append(toHold, workerPool.Submit(context.Background(), simpleGet))
	}
	assert.Equal(t, len(toHold), 10000)
	for _, holdMe := range toHold {
		result, err := holdMe.Get(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "a", result)
	}
	timeInRange(t, start, 1000, 0)
}

func Test_WorkerpoolFetching_NoCap2ScaleUps(t *testing.T) {
	start := time.Now()
	workerPool := NewDefaultSupplierWorkerPool[string](context.Background())
	workerPoolTyped := workerPool.(*defaultSupplierWorkerPool[string])
	timeToSleep := int64(1500)
	simpleGet := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		time.Sleep(time.Millisecond * time.Duration(timeToSleep))
		return "a", nil
	})
	// Single submit
	var toHold []async.Future[string]
	countToSubmit := 1000
	for i := 1; i <= countToSubmit; i++ {
		toHold = append(toHold, workerPool.Submit(context.Background(), simpleGet))
	}
	time.Sleep(time.Millisecond * 50)
	require.Equal(t, 0, workerPoolTyped.queue.Len(), time.Now().String())
	assert.Equal(t, 0, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, 1000, int(workerPoolTyped.totalCount.Load()))
	for _, holdMe := range toHold {
		result, err := holdMe.Get(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "a", result)
	}
	assert.Equal(t, 1000, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, 1000, int(workerPoolTyped.totalCount.Load()))
	timeInRange(t, start, timeToSleep*2, timeToSleep)
	assert.Equal(t, 0, workerPoolTyped.queue.Len())

	// And another mass submit does nothing
	toHold = []async.Future[string]{}
	for i := 1; i <= 1000; i++ {
		toHold = append(toHold, workerPool.Submit(context.Background(), simpleGet))
	}
	time.Sleep(time.Millisecond * 50)
	assert.True(t, workerPoolTyped.totalCount.Load() < 1500)
	assert.Equal(t, 0, workerPoolTyped.queue.Len())
	for _, holdMe := range toHold {
		result, err := holdMe.Get(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "a", result)
	}
	assert.Equal(t, 1000, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, 1000, int(workerPoolTyped.totalCount.Load()))
	timeInRange(t, start, timeToSleep*3, timeToSleep*2)
}

func Test_WorkerpoolFetching_LargeScaleUp(t *testing.T) {
	start := time.Now()
	workerPool := NewDefaultSupplierWorkerPool[string](context.Background())
	workerPoolTyped := workerPool.(*defaultSupplierWorkerPool[string])
	timeToSleep := int64(1500)
	simpleGet := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		time.Sleep(time.Millisecond * time.Duration(timeToSleep))
		return "a", nil
	})
	// Single submit
	var toHold []async.Future[string]
	countToSubmit := 10000
	for i := 1; i <= countToSubmit; i++ {
		toHold = append(toHold, workerPool.Submit(context.Background(), simpleGet))
	}
	time.Sleep(time.Millisecond * 50)
	require.Equal(t, 0, workerPoolTyped.queue.Len(), time.Now().String())
	assert.Equal(t, 0, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, countToSubmit, int(workerPoolTyped.totalCount.Load()))
	for _, holdMe := range toHold {
		result, err := holdMe.Get(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "a", result)
	}
	assert.Equal(t, countToSubmit, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, countToSubmit, int(workerPoolTyped.totalCount.Load()))
	timeInRange(t, start, timeToSleep*2, timeToSleep)
	assert.Equal(t, 0, workerPoolTyped.queue.Len())
}

func Test_SerialWorkerpoolFetching_WithPanics(t *testing.T) {
	workerPool := NewDefaultSupplierWorkerPool[string](context.Background(), WithMaxNumberOfThreads(1))
	workerPoolTyped := workerPool.(*defaultSupplierWorkerPool[string])
	noPanicYet := true
	simpleGet := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		if noPanicYet {
			noPanicYet = false
			panic("p")
		}
		return "a", nil
	})
	// Before doing anything
	assert.Equal(t, 0, workerPoolTyped.queue.Len())
	assert.Equal(t, 0, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, 0, int(workerPoolTyped.totalCount.Load()))
	// Single submit
	f1 := workerPool.Submit(context.Background(), simpleGet)
	result, err := f1.Get(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "", result)
	assert.Equal(t, 1, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, 1, int(workerPoolTyped.totalCount.Load()))
	// And another submit
	f1 = workerPool.Submit(context.Background(), simpleGet)
	result, err = f1.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
	assert.Equal(t, 1, int(workerPoolTyped.numberFree.Load()))
	assert.Equal(t, 1, int(workerPoolTyped.totalCount.Load()))
}

func Test_CanSubmitWithMetrics(t *testing.T) {
	ctxWithRegistry, registry := getContextWithRegistry()

	workerPool := NewDefaultSupplierWorkerPool[string](ctxWithRegistry, WithMetricTags(metrics.Tags{metrics.MustNewTag("k", "v")}))
	simpleGet := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "a", nil
	})
	// Single submit
	f1 := workerPool.Submit(context.Background(), simpleGet)
	result, err := f1.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
	workersReported := false
	queueLengthReported := false
	visitor := func(name string, tags metrics.Tags, value metrics.MetricVal) {
		valueAsCounter := value.Values()
		if name == cacheMetricName {
			workersReported = true
			assert.Equal(t, map[string]interface{}{"value": int64(1)}, valueAsCounter)
		} else if name == enqueuedMetricName {
			queueLengthReported = true
			assert.Equal(t, map[string]interface{}{"value": int64(0)}, valueAsCounter)
		} else {
			assert.Fail(t, "unknown metric encountered", name)
		}
		assert.Equal(t, 1, len(tags))
		userTag := tags[0]
		assert.Equal(t, metrics.MustNewTag("k", "v"), userTag)
	}
	registry.Each(visitor)
	assert.True(t, workersReported)
	assert.True(t, queueLengthReported)
}

func Test_WorkerPoolDedupesParentContextCancelation(t *testing.T) {
	// We submit no cancel first
	workerPool := NewDefaultSupplierWorkerPool[string](context.Background())
	getThatRespectsDoneContext := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "a", nil
	})
	ctx := context.Background()
	ctxThatIsDone, cancel := context.WithCancel(ctx)
	cancel()
	// Single submit
	f1 := workerPool.Submit(ctx, getThatRespectsDoneContext)
	result, err := f1.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
	// And now the broken one fails
	f1 = workerPool.Submit(ctxThatIsDone, getThatRespectsDoneContext)
	result, err = f1.Get(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "", result)

	// Now visa versa, just broke
	workerPool = NewDefaultSupplierWorkerPool[string](context.Background())
	f1 = workerPool.Submit(ctxThatIsDone, getThatRespectsDoneContext)
	result, err = f1.Get(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "", result)
	// And now the worker is just ruined
	f1 = workerPool.Submit(ctx, getThatRespectsDoneContext)
	result, err = f1.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
}

func Test_WorkerPoolStopsProcessingIfParentContextStopped(t *testing.T) {
	// We submit no cancel first
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	workerPool := NewDefaultSupplierWorkerPool[string](ctx)
	workerPoolTyped := workerPool.(*defaultSupplierWorkerPool[string])
	getFunc := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "a", nil
	})
	// Single submit
	f1 := workerPool.Submit(context.Background(), getFunc)
	result, err := f1.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
	// And now stop it
	assert.False(t, workerPoolTyped.queue.ShuttingDown())
	cancel()
	time.Sleep(time.Millisecond * 100)
	assert.True(t, workerPoolTyped.queue.ShuttingDown())
}

func timeInRange(t *testing.T, start time.Time, milliLessThan, milliMoreThan int64) {
	timeTaken := time.Now().Sub(start)
	assert.True(t, timeTaken.Milliseconds() < milliLessThan, fmt.Sprintf("time taken: %d compared to milliLessThan: %d", timeTaken.Milliseconds(), milliLessThan))
	assert.True(t, timeTaken.Milliseconds() > milliMoreThan, fmt.Sprintf("time taken: %d compared to milliMoreThan: %d", timeTaken.Milliseconds(), milliMoreThan))
}

func getContextWithRegistry() (context.Context, metrics.Registry) {
	registry := metrics.NewRootMetricsRegistry()
	ctx := context.Background()
	ctx = metrics.WithRegistry(ctx, registry)
	return ctx, registry
}

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

package internal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/stretchr/testify/assert"
)

func Test_ComputeCanRunWithNoError(t *testing.T) {
	toGet := func(ctx context.Context) (string, error) {
		return "a", nil
	}
	compute := NewDefaultComputingFuture(function.NewSupplierFromFunc(toGet))
	compute.Compute(context.Background())
	result, err := compute.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
}

func Test_ComputeCanRunWithError(t *testing.T) {
	toGet := func(ctx context.Context) (string, error) {
		return "", werror.Error("err")
	}
	compute := NewDefaultComputingFuture(function.NewSupplierFromFunc(toGet))
	compute.Compute(context.Background())
	result, err := compute.Get(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "", result)
}

func Test_ComputeWillNeverRunTwice_GetCan(t *testing.T) {
	timesCalled := 0
	toGet := func(ctx context.Context) (string, error) {
		timesCalled = timesCalled + 1
		return "a", nil
	}
	compute := NewDefaultComputingFuture(function.NewSupplierFromFunc(toGet))
	compute.Compute(context.Background())
	compute.Compute(context.Background())
	result, err := compute.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
	assert.Equal(t, 1, timesCalled)
	result, err = compute.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
	assert.Equal(t, 1, timesCalled)
}

func Test_GetBeforeCompute(t *testing.T) {
	toGet := func(ctx context.Context) (string, error) {
		return "a", nil
	}
	compute := NewDefaultComputingFuture(function.NewSupplierFromFunc(toGet))
	waitPeriod := time.Millisecond * 101
	start := time.Now()
	go func() {
		time.Sleep(waitPeriod)
		compute.Compute(context.Background())
	}()
	result, err := compute.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
	timeTaken := time.Now().Sub(start)
	assert.True(t, timeTaken.Milliseconds() < 200)
	assert.True(t, timeTaken.Milliseconds() > 100)
}

// Verifies that if multiple goroutines are all waiting on Get, they are all unblocked when Compute is called
func Test_MultipleGetCalls(t *testing.T) {
	called := new(atomic.Bool)
	toGet := func(ctx context.Context) (string, error) {
		if !called.CompareAndSwap(false, true) {
			return "", errors.New("underlying compute function should be called only once")
		}
		return "a", nil
	}
	compute := NewDefaultComputingFuture(function.NewSupplierFromFunc(toGet))
	getWithDuration := func(start time.Time) (string, time.Duration, error) {
		result, err := compute.Get(context.Background())
		if err != nil {
			return "", 0, err
		}
		return result, time.Since(start), nil
	}
	waitPeriod := time.Millisecond * 101
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			result, timeTaken, err := getWithDuration(start)
			assert.NoError(t, err)
			assert.Equal(t, "a", result)
			assert.Less(t, timeTaken.Milliseconds(), int64(200), "Get should be quickly unblocked by Compute")
			assert.Greater(t, timeTaken.Milliseconds(), int64(100), "Get should be blocked by Compute")
		}()
	}

	time.Sleep(waitPeriod)
	assert.False(t, called.Load(), "toGet should not yet have been called")
	compute.Compute(context.Background())
	wg.Wait()
	assert.True(t, called.Load(), "toGet should have been called")

	// Check that Get after Compute is instant
	result, timeTaken, err := getWithDuration(time.Now())
	assert.NoError(t, err)
	assert.Equal(t, "a", result)
	assert.Less(t, timeTaken.Milliseconds(), int64(10), "Get should be already unblocked")
}

func Test_CanReturnPanic(t *testing.T) {
	toGet := func(ctx context.Context) (string, error) {
		panic("a")
	}
	compute := NewDefaultComputingFuture(function.NewSupplierFromFunc(toGet))
	compute.Compute(context.Background())
	result, err := compute.Get(context.Background())
	assert.EqualError(t, err, "panic recovered")
	assert.Equal(t, "", result)
}

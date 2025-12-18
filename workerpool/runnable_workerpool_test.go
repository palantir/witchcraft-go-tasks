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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/stretchr/testify/assert"
)

func TestRunnableWorkerPool_Submit(t *testing.T) {
	t.Run("executes runnable successfully", func(t *testing.T) {
		pool := NewDefaultRunnableWorkerPool(context.Background())
		var executed atomic.Bool
		runnable := function.NewRunnableFromFunc(func(ctx context.Context) error {
			executed.Store(true)
			return nil
		})
		future := pool.Submit(context.Background(), runnable)
		_, err := future.Get(context.Background())
		assert.NoError(t, err)
		assert.True(t, executed.Load())
	})
	t.Run("returns error from runnable", func(t *testing.T) {
		pool := NewDefaultRunnableWorkerPool(context.Background())
		expectedErr := errors.New("runnable error")
		runnable := function.NewRunnableFromFunc(func(ctx context.Context) error {
			return expectedErr
		})
		future := pool.Submit(context.Background(), runnable)
		_, err := future.Get(context.Background())
		assert.ErrorIs(t, err, expectedErr)
	})
}

func TestRunnableWorkerPool_SubmitWithCallback(t *testing.T) {
	t.Run("calls callback on success", func(t *testing.T) {
		pool := NewDefaultRunnableWorkerPool(context.Background())
		var callbackCalled atomic.Bool
		var callbackHadNoErr atomic.Bool
		runnable := function.NewRunnableFromFunc(func(ctx context.Context) error {
			return nil
		})
		pool.SubmitWithCallback(context.Background(), runnable, func(ctx context.Context, err error) {
			callbackHadNoErr.Store(err == nil)
			callbackCalled.Store(true)
		})
		assert.Eventually(t, func() bool {
			return callbackCalled.Load()
		}, time.Millisecond*100, time.Millisecond*10)
		assert.True(t, callbackHadNoErr.Load())
	})
	t.Run("calls callback with error", func(t *testing.T) {
		pool := NewDefaultRunnableWorkerPool(context.Background())
		expectedErr := errors.New("runnable error")
		var callbackCalled atomic.Bool
		var callbackErr atomic.Value
		runnable := function.NewRunnableFromFunc(func(ctx context.Context) error {
			return expectedErr
		})
		pool.SubmitWithCallback(context.Background(), runnable, func(ctx context.Context, err error) {
			callbackErr.Store(err)
			callbackCalled.Store(true)
		})
		assert.Eventually(t, func() bool {
			return callbackCalled.Load()
		}, time.Millisecond*100, time.Millisecond*10)
		assert.Equal(t, expectedErr, callbackErr.Load())
	})
}

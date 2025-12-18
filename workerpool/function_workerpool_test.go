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

func TestFunctionWorkerPool_Submit(t *testing.T) {
	t.Run("executes function and returns result", func(t *testing.T) {
		fn := function.NewFunctionFromFunc(func(ctx context.Context, arg int) (string, error) {
			return "result", nil
		})
		pool := NewDefaultFunctionWorkerPool(context.Background(), fn)
		future := pool.Submit(context.Background(), 42)
		result, err := future.Get(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "result", result)
	})
	t.Run("passes argument to function", func(t *testing.T) {
		var receivedArg atomic.Int32
		fn := function.NewFunctionFromFunc(func(ctx context.Context, arg int) (string, error) {
			receivedArg.Store(int32(arg))
			return "result", nil
		})
		pool := NewDefaultFunctionWorkerPool(context.Background(), fn)
		future := pool.Submit(context.Background(), 42)
		_, err := future.Get(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, int32(42), receivedArg.Load())
	})
	t.Run("returns error from function", func(t *testing.T) {
		expectedErr := errors.New("function error")
		fn := function.NewFunctionFromFunc(func(ctx context.Context, arg int) (string, error) {
			return "", expectedErr
		})
		pool := NewDefaultFunctionWorkerPool(context.Background(), fn)
		future := pool.Submit(context.Background(), 42)
		result, err := future.Get(context.Background())
		assert.ErrorIs(t, err, expectedErr)
		assert.Equal(t, "", result)
	})
}

func TestFunctionWorkerPool_SubmitWithCallback(t *testing.T) {
	t.Run("calls callback with result on success", func(t *testing.T) {
		fn := function.NewFunctionFromFunc(func(ctx context.Context, arg int) (string, error) {
			return "result", nil
		})
		pool := NewDefaultFunctionWorkerPool(context.Background(), fn)
		var callbackCalled atomic.Bool
		var callbackArg atomic.Int32
		var callbackResult atomic.Value
		var callbackHadNoErr atomic.Bool
		pool.SubmitWithCallback(context.Background(), 42, func(ctx context.Context, arg int, result string, err error) {
			callbackArg.Store(int32(arg))
			callbackResult.Store(result)
			callbackHadNoErr.Store(err == nil)
			callbackCalled.Store(true)
		})
		assert.Eventually(t, func() bool {
			return callbackCalled.Load()
		}, time.Millisecond*100, time.Millisecond*10)
		assert.Equal(t, int32(42), callbackArg.Load())
		assert.Equal(t, "result", callbackResult.Load())
		assert.True(t, callbackHadNoErr.Load())
	})
	t.Run("calls callback with error", func(t *testing.T) {
		expectedErr := errors.New("function error")
		fn := function.NewFunctionFromFunc(func(ctx context.Context, arg int) (string, error) {
			return "", expectedErr
		})
		pool := NewDefaultFunctionWorkerPool(context.Background(), fn)
		var callbackCalled atomic.Bool
		var callbackArg atomic.Int32
		var callbackResult atomic.Value
		var callbackErr atomic.Value
		pool.SubmitWithCallback(context.Background(), 42, func(ctx context.Context, arg int, result string, err error) {
			callbackArg.Store(int32(arg))
			callbackResult.Store(result)
			callbackErr.Store(err)
			callbackCalled.Store(true)
		})
		assert.Eventually(t, func() bool {
			return callbackCalled.Load()
		}, time.Millisecond*100, time.Millisecond*10)
		assert.Equal(t, int32(42), callbackArg.Load())
		assert.Equal(t, "", callbackResult.Load())
		assert.Equal(t, expectedErr, callbackErr.Load())
	})
}

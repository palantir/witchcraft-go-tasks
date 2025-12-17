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

func TestConsumerWorkerPool_Submit(t *testing.T) {
	t.Run("executes consumer with argument", func(t *testing.T) {
		var receivedArg atomic.Value
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, arg string) error {
			receivedArg.Store(arg)
			return nil
		})
		pool := NewDefaultConsumerWorkerPool(context.Background(), consumer)
		future := pool.Submit(context.Background(), "test-arg")
		_, err := future.Get(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "test-arg", receivedArg.Load())
	})
	t.Run("returns error from consumer", func(t *testing.T) {
		expectedErr := errors.New("consumer error")
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, arg string) error {
			return expectedErr
		})
		pool := NewDefaultConsumerWorkerPool(context.Background(), consumer)
		future := pool.Submit(context.Background(), "test-arg")
		_, err := future.Get(context.Background())
		assert.ErrorIs(t, err, expectedErr)
	})
}

func TestConsumerWorkerPool_SubmitWithCallback(t *testing.T) {
	t.Run("calls callback with argument on success", func(t *testing.T) {
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, arg string) error {
			return nil
		})
		pool := NewDefaultConsumerWorkerPool(context.Background(), consumer)
		var callbackCalled atomic.Bool
		var callbackArg atomic.Value
		var callbackHadNoErr atomic.Bool
		pool.SubmitWithCallback(context.Background(), "test-arg", func(ctx context.Context, arg string, err error) {
			callbackArg.Store(arg)
			callbackHadNoErr.Store(err == nil)
			callbackCalled.Store(true)
		})
		assert.Eventually(t, func() bool {
			return callbackCalled.Load()
		}, time.Millisecond*100, time.Millisecond*10)
		assert.Equal(t, "test-arg", callbackArg.Load())
		assert.True(t, callbackHadNoErr.Load())
	})
	t.Run("calls callback with error", func(t *testing.T) {
		expectedErr := errors.New("consumer error")
		consumer := function.NewConsumerFromFunc(func(ctx context.Context, arg string) error {
			return expectedErr
		})
		pool := NewDefaultConsumerWorkerPool(context.Background(), consumer)
		var callbackCalled atomic.Bool
		var callbackArg atomic.Value
		var callbackErr atomic.Value
		pool.SubmitWithCallback(context.Background(), "test-arg", func(ctx context.Context, arg string, err error) {
			callbackArg.Store(arg)
			callbackErr.Store(err)
			callbackCalled.Store(true)
		})
		assert.Eventually(t, func() bool {
			return callbackCalled.Load()
		}, time.Millisecond*100, time.Millisecond*10)
		assert.Equal(t, "test-arg", callbackArg.Load())
		assert.Equal(t, expectedErr, callbackErr.Load())
	})
}

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
	"github.com/palantir/witchcraft-go-tasks/internal/testcontext"
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

	t.Run("calls completion once after panics and processes subsequent work", func(t *testing.T) {
		ctx, cancel := context.WithCancel(testcontext.GetTestContext(t))
		defer cancel()

		var executions atomic.Int32
		consumer := function.NewConsumerFromFunc(func(_ context.Context, arg string) error {
			executions.Add(1)
			if arg == "consumer-panic" {
				// Simulate a consumer failure. Completion must still run
				// with an error even though the consumer panics.
				panic("consumer panicked")
			}
			return nil
		})

		// Use one worker so follow-up work cannot rely on spare capacity
		// to hide a worker that became stuck after a panic.
		pool := NewDefaultConsumerWorkerPool(
			ctx, consumer, WithMaxNumberOfWorkers(1),
		)

		var panicCalls atomic.Int32
		var panicErr atomic.Value
		pool.SubmitWithCallback(ctx, "consumer-panic",
			func(callbackCtx context.Context, arg string, err error) {
				panicCalls.Add(1)
				assert.Same(t, ctx, callbackCtx)
				assert.Equal(t, "consumer-panic", arg)
				assert.ErrorContains(t, err, "panic recovered")
				if err != nil {
					panicErr.Store(err)
				}
			},
		)

		var panickingCallbackCalls atomic.Int32
		var callbackHadNoErr atomic.Bool
		pool.SubmitWithCallback(ctx, "callback-panic",
			func(_ context.Context, _ string, err error) {
				panickingCallbackCalls.Add(1)
				callbackHadNoErr.Store(err == nil)

				// Completion can also contain user code that panics.
				// Recovery must not invoke this callback a second time.
				panic("completion panicked")
			},
		)

		var subsequentHadNoErr, subsequentCalled atomic.Bool
		pool.SubmitWithCallback(ctx, "subsequent",
			func(_ context.Context, _ string, err error) {
				subsequentHadNoErr.Store(err == nil)

				// Publish readiness last so the test sees all callback results.
				subsequentCalled.Store(true)
			},
		)

		// With one worker, reaching the final callback means the earlier
		// jobs and callbacks have finished, so we can check their outcomes.
		assert.Eventually(t, subsequentCalled.Load, time.Second, 50*time.Millisecond)

		// All three jobs must reach the consumer despite the earlier panics.
		assert.Equal(t, int32(3), executions.Load())

		// Recovery must deliver completion exactly once after a consumer
		// panic. This fails if recovery catches it but skips completion.
		assert.Equal(t, int32(1), panicCalls.Load())

		// Completion must report the recovered panic as an error.
		// The safe conversion also handles a missing callback after a timeout.
		recoveredErr, _ := panicErr.Load().(error)
		assert.Error(t, recoveredErr)

		// A panic in completion must not cause that callback to be retried.
		assert.Equal(t, int32(1), panickingCallbackCalls.Load(),
			"a panicking completion must not be invoked again")

		// This job's consumer succeeded; its panic came from completion.
		assert.True(t, callbackHadNoErr.Load())

		// The pool must remain usable after both kinds of failure.
		assert.True(t, subsequentHadNoErr.Load(),
			"the single worker must process subsequent work")
	})
}

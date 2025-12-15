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

package jobs

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palantir/witchcraft-go-health/conjure/witchcraft/api/health"
	"github.com/palantir/witchcraft-go-health/sources/window"
	"github.com/palantir/witchcraft-go-tasks/internal/testcontext"
	"github.com/palantir/witchcraft-go-tasks/runnable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDefaultJobRunner(t *testing.T) {
	healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
	runner := NewDefaultJobRunner(healthCheckSource)
	assert.NotNil(t, runner)
}

func TestDefaultJobRunner_StartJobs(t *testing.T) {
	t.Run("starts job that runs on interval", func(t *testing.T) {
		ctx, cancel := context.WithCancel(testcontext.GetTestContext(t))
		defer cancel()
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		runner := NewDefaultJobRunner(healthCheckSource)
		var count atomic.Int32
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			count.Add(1)
			return nil
		})
		job := NewDefaultJob("test-job", r, WithInterval(50*time.Millisecond))
		runner.StartJobs(ctx, []Job{job})
		assert.Eventually(t, func() bool {
			return count.Load() >= 3
		}, 500*time.Millisecond, 10*time.Millisecond)
	})
	t.Run("starts job immediately when configured", func(t *testing.T) {
		ctx, cancel := context.WithCancel(testcontext.GetTestContext(t))
		defer cancel()
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		runner := NewDefaultJobRunner(healthCheckSource)
		var executed atomic.Bool
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			executed.Store(true)
			return nil
		})
		job := NewDefaultJob("test-job", r,
			WithInterval(time.Hour),
			WithStartImmediately(true),
		)
		runner.StartJobs(ctx, []Job{job})
		assert.Eventually(t, func() bool {
			return executed.Load()
		}, 100*time.Millisecond, 5*time.Millisecond)
	})
	t.Run("calls error logger on job failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(testcontext.GetTestContext(t))
		defer cancel()
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		runner := NewDefaultJobRunner(healthCheckSource)
		expectedErr := errors.New("job error")
		var loggedErr atomic.Value
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			return expectedErr
		})
		job := NewDefaultJob("test-job", r,
			WithInterval(50*time.Millisecond),
			WithErrorLogger(func(ctx context.Context, err error) {
				loggedErr.Store(err)
			}),
		)
		runner.StartJobs(ctx, []Job{job})
		assert.Eventually(t, func() bool {
			v := loggedErr.Load()
			if v == nil {
				return false
			}
			return v.(error) == expectedErr
		}, 200*time.Millisecond, 10*time.Millisecond)
	})
	t.Run("stops job when context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(testcontext.GetTestContext(t))
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		runner := NewDefaultJobRunner(healthCheckSource)
		var count atomic.Int32
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			count.Add(1)
			return nil
		})
		job := NewDefaultJob("test-job", r, WithInterval(50*time.Millisecond))
		runner.StartJobs(ctx, []Job{job})
		assert.Eventually(t, func() bool {
			return count.Load() >= 2
		}, 500*time.Millisecond, 10*time.Millisecond)
		cancel()
		countAtCancel := count.Load()
		assert.Eventually(t, func() bool {
			return count.Load() == countAtCancel
		}, 200*time.Millisecond, 50*time.Millisecond, "job should stop after context cancellation")
	})
	t.Run("starts multiple jobs", func(t *testing.T) {
		ctx, cancel := context.WithCancel(testcontext.GetTestContext(t))
		defer cancel()
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		runner := NewDefaultJobRunner(healthCheckSource)
		var executed1, executed2 atomic.Bool
		r1 := runnable.New("test-runnable-1", func(ctx context.Context) error {
			executed1.Store(true)
			return nil
		})
		r2 := runnable.New("test-runnable-2", func(ctx context.Context) error {
			executed2.Store(true)
			return nil
		})
		job1 := NewDefaultJob("test-job-1", r1, WithInterval(time.Hour), WithStartImmediately(true))
		job2 := NewDefaultJob("test-job-2", r2, WithInterval(time.Hour), WithStartImmediately(true))
		runner.StartJobs(ctx, []Job{job1, job2})
		assert.Eventually(t, func() bool {
			return executed1.Load() && executed2.Load()
		}, 100*time.Millisecond, 5*time.Millisecond)
	})
	t.Run("submits health check on job execution", func(t *testing.T) {
		ctx, cancel := context.WithCancel(testcontext.GetTestContext(t))
		defer cancel()
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		runner := NewDefaultJobRunner(healthCheckSource)
		var executed atomic.Bool
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			executed.Store(true)
			return nil
		})
		job := NewDefaultJob("test-job", r, WithInterval(time.Hour), WithStartImmediately(true))
		runner.StartJobs(ctx, []Job{job})
		require.Eventually(t, func() bool {
			return executed.Load()
		}, 100*time.Millisecond, 5*time.Millisecond)
		var status health.HealthStatus
		var mu sync.Mutex
		assert.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			status = healthCheckSource.HealthStatus(ctx)
			_, ok := status.Checks[health.CheckType("test")]
			return ok
		}, 100*time.Millisecond, 5*time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, health.HealthState_HEALTHY, status.Checks[health.CheckType("test")].State.Value())
	})
	t.Run("submits error to health check on job failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(testcontext.GetTestContext(t))
		defer cancel()
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		runner := NewDefaultJobRunner(healthCheckSource)
		var executed atomic.Bool
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			executed.Store(true)
			return errors.New("job failed")
		})
		job := NewDefaultJob("test-job", r,
			WithInterval(time.Hour),
			WithStartImmediately(true),
			WithErrorLogger(func(ctx context.Context, err error) {}),
		)
		runner.StartJobs(ctx, []Job{job})
		require.Eventually(t, func() bool {
			return executed.Load()
		}, 100*time.Millisecond, 5*time.Millisecond)
		var status health.HealthStatus
		var mu sync.Mutex
		assert.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			status = healthCheckSource.HealthStatus(ctx)
			check, ok := status.Checks[health.CheckType("test")]
			return ok && check.State.Value() == health.HealthState_ERROR
		}, 100*time.Millisecond, 5*time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, health.HealthState_ERROR, status.Checks[health.CheckType("test")].State.Value())
	})
	t.Run("recovers from panic and continues running", func(t *testing.T) {
		ctx, cancel := context.WithCancel(testcontext.GetTestContext(t))
		defer cancel()
		healthCheckSource := window.MustNewKeyedErrorHealthCheckSource(health.CheckType("test"), window.UnhealthyIfAtLeastOneError)
		runner := NewDefaultJobRunner(healthCheckSource)
		var count atomic.Int32
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			c := count.Add(1)
			if c == 1 {
				panic("first iteration panic")
			}
			return nil
		})
		job := NewDefaultJob("test-job", r,
			WithInterval(50*time.Millisecond),
			WithStartImmediately(true),
			WithErrorLogger(func(ctx context.Context, err error) {}),
		)
		runner.StartJobs(ctx, []Job{job})
		assert.Eventually(t, func() bool {
			return count.Load() >= 2
		}, 500*time.Millisecond, 10*time.Millisecond, "job should continue running after panic")
	})
}

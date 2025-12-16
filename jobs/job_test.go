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
	"testing"
	"time"

	"github.com/palantir/witchcraft-go-tasks/internal/testcontext"
	"github.com/palantir/witchcraft-go-tasks/runnable"
	"github.com/stretchr/testify/assert"
)

func TestNewDefaultJob(t *testing.T) {
	t.Run("creates job with default values", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			return nil
		})
		job := NewDefaultJob("test-job", r)
		assert.Equal(t, "test-job", job.Name())
		assert.Equal(t, time.Minute, job.GetInterval(ctx))
		assert.False(t, job.ShouldStartImmediately(ctx))
	})
	t.Run("creates job with custom interval", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			return nil
		})
		job := NewDefaultJob("test-job", r, WithInterval(5*time.Second))
		assert.Equal(t, 5*time.Second, job.GetInterval(ctx))
	})
	t.Run("creates job with start immediately", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			return nil
		})
		job := NewDefaultJob("test-job", r, WithStartImmediately(true))
		assert.True(t, job.ShouldStartImmediately(ctx))
	})
	t.Run("creates job with custom error logger", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		var loggedErr error
		errorLogger := func(ctx context.Context, err error) {
			loggedErr = err
		}
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			return nil
		})
		job := NewDefaultJob("test-job", r, WithErrorLogger(errorLogger))
		testErr := errors.New("test error")
		job.LogError(ctx, testErr)
		assert.Equal(t, testErr, loggedErr)
	})
	t.Run("creates job with multiple options", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		var loggedErr error
		errorLogger := func(ctx context.Context, err error) {
			loggedErr = err
		}
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			return nil
		})
		job := NewDefaultJob("test-job", r,
			WithInterval(10*time.Second),
			WithStartImmediately(true),
			WithErrorLogger(errorLogger),
		)
		assert.Equal(t, "test-job", job.Name())
		assert.Equal(t, 10*time.Second, job.GetInterval(ctx))
		assert.True(t, job.ShouldStartImmediately(ctx))
		testErr := errors.New("test error")
		job.LogError(ctx, testErr)
		assert.Equal(t, testErr, loggedErr)
	})
}

func TestDefaultJob_Run(t *testing.T) {
	t.Run("runs the underlying runnable successfully", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		executed := false
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			executed = true
			return nil
		})
		job := NewDefaultJob("test-job", r)
		err := job.Run(ctx)
		assert.NoError(t, err)
		assert.True(t, executed)
	})
	t.Run("returns error from underlying runnable", func(t *testing.T) {
		ctx := testcontext.GetTestContext(t)
		expectedErr := errors.New("runnable error")
		r := runnable.New("test-runnable", func(ctx context.Context) error {
			return expectedErr
		})
		job := NewDefaultJob("test-job", r)
		err := job.Run(ctx)
		assert.Equal(t, expectedErr, err)
	})
}

// Copyright (c) 2026 Palantir Technologies. All rights reserved.
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
	"sync/atomic"
	"testing"

	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/workerpool"
	"github.com/stretchr/testify/assert"
)

func TestExecuteRunnables(t *testing.T) {
	voidWorkerPool := workerpool.NewDefaultRunnableWorkerPool(context.Background())
	runnableExecutor := NewDefaultRunnableExecutor(voidWorkerPool)
	var r1Called atomic.Bool
	var r2Called atomic.Bool
	r1 := function.NewRunnableFromFunc(func(ctx context.Context) error {
		r1Called.Store(true)
		return nil
	})
	r2 := function.NewRunnableFromFunc(func(ctx context.Context) error {
		r2Called.Store(true)
		return nil
	})
	errs := runnableExecutor.ExecuteRunnables(context.Background(), []function.Runnable{r1, r2})
	assert.Empty(t, errs)
	assert.True(t, r1Called.Load())
	assert.True(t, r2Called.Load())
}

func TestExecuteRunnables_WithError(t *testing.T) {
	voidWorkerPool := workerpool.NewDefaultRunnableWorkerPool(context.Background())
	runnableExecutor := NewDefaultRunnableExecutor(voidWorkerPool)
	var r1Called atomic.Bool
	r1 := function.NewRunnableFromFunc(func(ctx context.Context) error {
		r1Called.Store(true)
		return nil
	})
	r2 := function.NewRunnableFromFunc(func(ctx context.Context) error {
		return werror.Error("err here")
	})
	errs := runnableExecutor.ExecuteRunnables(context.Background(), []function.Runnable{r1, r2})
	assert.Equal(t, 1, len(errs))
	assert.EqualError(t, errs[0], "err here")
	assert.True(t, r1Called.Load())
}

func TestExecuteRunnable(t *testing.T) {
	voidWorkerPool := workerpool.NewDefaultRunnableWorkerPool(context.Background())
	runnableExecutor := NewDefaultRunnableExecutor(voidWorkerPool)
	var r1Called atomic.Bool
	r1 := function.NewRunnableFromFunc(func(ctx context.Context) error {
		r1Called.Store(true)
		return nil
	})
	err := runnableExecutor.ExecuteRunnable(context.Background(), r1)
	assert.NoError(t, err)
	assert.True(t, r1Called.Load())
}

func TestExecuteRunnable_WithError(t *testing.T) {
	voidWorkerPool := workerpool.NewDefaultRunnableWorkerPool(context.Background())
	runnableExecutor := NewDefaultRunnableExecutor(voidWorkerPool)
	r1 := function.NewRunnableFromFunc(func(ctx context.Context) error {
		return werror.Error("err here")
	})
	err := runnableExecutor.ExecuteRunnable(context.Background(), r1)
	assert.EqualError(t, err, "err here")
}

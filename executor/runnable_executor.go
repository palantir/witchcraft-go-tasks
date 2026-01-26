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

	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/util/async"
	"github.com/palantir/witchcraft-go-tasks/workerpool"
)

// RunnableExecutor provides a high-level interface for executing multiple Runnable tasks in parallel.
//
// The executor submits all runnables to an underlying worker pool, allowing them to execute concurrently.
// The degree of parallelism is controlled by the workerpool.RunnableWorkerPool provided at construction time.
// All runnables are guaranteed to complete (or fail) before the execute methods return.
//
// This is useful for fire-and-forget style operations where you need to run multiple independent tasks
// and only care about whether they succeeded or failed, not about return values.
type RunnableExecutor interface {
	// ExecuteRunnables submits all provided runnables to the worker pool for parallel execution.
	// It blocks until all runnables have completed and returns a slice of all errors encountered.
	// The returned error slice maintains the order of errors as they were collected (not necessarily
	// the order of the input runnables). If all runnables succeed, an empty slice is returned.
	ExecuteRunnables(ctx context.Context, runnables []function.Runnable) []error
	// ExecuteRunnable submits a single runnable to the worker pool for execution.
	// It blocks until the runnable completes and returns its error (or nil on success).
	// This is a convenience method equivalent to calling ExecuteRunnables with a single-element slice.
	ExecuteRunnable(ctx context.Context, runnable function.Runnable) error
}

type defaultRunnableExecutor struct {
	voidWorkerPool workerpool.RunnableWorkerPool
}

// NewDefaultRunnableExecutor creates a new RunnableExecutor that uses the provided worker pool
// for task execution. The worker pool controls the maximum concurrency of runnable execution.
// For unbounded parallelism, use workerpool.NewDefaultRunnableWorkerPool(ctx).
// For bounded parallelism, use workerpool.NewDefaultRunnableWorkerPool(ctx, workerpool.WithMaxNumberOfWorkers(n)).
func NewDefaultRunnableExecutor(voidWorkerPool workerpool.RunnableWorkerPool) RunnableExecutor {
	return &defaultRunnableExecutor{
		voidWorkerPool: voidWorkerPool,
	}
}

func (d *defaultRunnableExecutor) ExecuteRunnables(ctx context.Context, runnables []function.Runnable) []error {
	var futures []async.VoidFuture
	for _, runnable := range runnables {
		futures = append(futures, d.voidWorkerPool.Submit(ctx, runnable))
	}
	var errList []error
	for _, future := range futures {
		_, err := future.Get(ctx)
		if err != nil {
			errList = append(errList, err)
		}
	}
	return errList
}

func (d *defaultRunnableExecutor) ExecuteRunnable(ctx context.Context, runnable function.Runnable) error {
	errList := d.ExecuteRunnables(ctx, []function.Runnable{runnable})
	if len(errList) == 0 {
		return nil
	}
	return errList[0]
}

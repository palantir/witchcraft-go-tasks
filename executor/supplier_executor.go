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

// SupplierExecutor provides a high-level interface for executing multiple Supplier tasks in parallel
// and collecting their results.
//
// The executor submits all suppliers to an underlying worker pool, allowing them to execute concurrently.
// The degree of parallelism is controlled by the workerpool.SupplierWorkerPool provided at construction time.
// All suppliers are guaranteed to complete (or fail) before the resolve methods return.
//
// This is useful when you need to fetch or compute multiple values in parallel and aggregate the results.
// Two resolution strategies are provided: one that collects all results regardless of errors (ResolveAll),
// and one that fails fast on the first error (ResolveUntilError).
type SupplierExecutor[T any] interface {
	// ResolveAll submits all provided suppliers to the worker pool for parallel execution.
	// It blocks until all suppliers have completed and returns two slices:
	//   - A slice of successful results (values from suppliers that did not error)
	//   - A slice of errors (from suppliers that failed)
	// The successful results maintain their relative order from the input slice.
	// Use this method when you want to collect as many results as possible, even if some suppliers fail.
	ResolveAll(ctx context.Context, suppliers []function.Supplier[T]) ([]T, []error)
	// ResolveUntilError submits all provided suppliers to the worker pool for parallel execution.
	// It blocks until all suppliers have completed, then inspects results in order.
	// If all suppliers succeed, returns a slice of all results and a nil error.
	// If any supplier fails, returns nil and the first error encountered (in input order).
	// Note: All suppliers will execute regardless of errors; "until error" refers to result collection,
	// not execution. Use this method when partial results are not useful and you need all-or-nothing semantics.
	ResolveUntilError(ctx context.Context, suppliers []function.Supplier[T]) ([]T, error)
}

// NewDefaultSupplierExecutor creates a new SupplierExecutor that uses the provided worker pool
// for task execution. The worker pool controls the maximum concurrency of supplier execution.
// For unbounded parallelism, use workerpool.NewDefaultSupplierWorkerPool[T](ctx).
// For bounded parallelism, use workerpool.NewDefaultSupplierWorkerPool[T](ctx, workerpool.WithMaxNumberOfWorkers(n)).
func NewDefaultSupplierExecutor[T any](workerPool workerpool.SupplierWorkerPool[T]) SupplierExecutor[T] {
	return &defaultSupplierExecutor[T]{
		workerPool: workerPool,
	}
}

type defaultSupplierExecutor[T any] struct {
	workerPool workerpool.SupplierWorkerPool[T]
}

func (d *defaultSupplierExecutor[T]) ResolveAll(ctx context.Context, suppliers []function.Supplier[T]) ([]T, []error) {
	futuresToRun := d.collectFutures(ctx, suppliers)
	var toReturn []T
	var errors []error
	for _, result := range futuresToRun {
		v, err := result.Get(ctx)
		if err != nil {
			errors = append(errors, err)
		} else {
			toReturn = append(toReturn, v)
		}

	}
	return toReturn, errors
}

func (d *defaultSupplierExecutor[T]) ResolveUntilError(ctx context.Context, suppliers []function.Supplier[T]) ([]T, error) {
	futuresToRun := d.collectFutures(ctx, suppliers)
	var toReturn []T
	for _, result := range futuresToRun {
		v, err := result.Get(ctx)
		if err != nil {
			return nil, err
		}
		toReturn = append(toReturn, v)
	}
	return toReturn, nil
}

func (d *defaultSupplierExecutor[T]) collectFutures(ctx context.Context, suppliers []function.Supplier[T]) []async.Future[T] {
	var futuresToRun []async.Future[T]
	for _, toCall := range suppliers {
		futuresToRun = append(futuresToRun, d.getFuture(ctx, toCall))
	}
	return futuresToRun
}

func (d *defaultSupplierExecutor[T]) getFuture(ctx context.Context, call function.Supplier[T]) async.Future[T] {
	return d.workerPool.Submit(ctx, call)
}

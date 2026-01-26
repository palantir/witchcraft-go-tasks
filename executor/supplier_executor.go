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

// SupplierExecutor is a type of executor that takes in functional.Supplier as arguments and returns the values those suppliers supply
type SupplierExecutor[T any] interface {
	// ResolveAll runs all functions that are submitted and ran
	// Each result is inspected and they are split between success results and failures, both are returned
	ResolveAll(ctx context.Context, suppliers []function.Supplier[T]) ([]T, []error)
	// ResolveUntilError runs all functions that are submitted  and ran
	// Each result is inspected and success are kept. If all suppliers result in a success, the entire slice is returned as well as a nil error
	// If any supplier errors, the first error is returned and no results are returned
	ResolveUntilError(ctx context.Context, suppliers []function.Supplier[T]) ([]T, error)
}

// NewDefaultSupplierExecutor returns the default implemntation of SupplierExecutor
// In this implementation, the throughput is bound by the given workerPool
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

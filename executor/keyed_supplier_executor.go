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

// KeyedSupplierExecutor is the map-shaped sibling of SupplierExecutor[T]. It submits a
// map of Supplier tasks (keyed by K) to a worker pool for parallel execution and
// collects the results into a map[K]T.
//
// Use it when each task has a meaningful identifier — e.g. a resource ID or entity
// key — that should be preserved on the result rather than positional order.
//
// Error semantics: ResolveAll is fail-fast. The first supplier error stops result
// collection and is returned to the caller (other suppliers may still execute to
// completion inside the worker pool, but their results are discarded). This is
// different from SupplierExecutor.ResolveAll, which collects errors alongside
// successful results.
type KeyedSupplierExecutor[K comparable, T any] interface {
	ResolveAll(ctx context.Context, suppliers map[K]function.Supplier[T]) (map[K]T, error)
}

type defaultKeyedSupplierExecutor[K comparable, T any] struct {
	workerPool workerpool.SupplierWorkerPool[T]
}

// NewDefaultKeyedSupplierExecutor returns the default implementation of
// KeyedSupplierExecutor[K, T]. The worker pool controls the maximum concurrency of
// supplier execution.
func NewDefaultKeyedSupplierExecutor[K comparable, T any](workerPool workerpool.SupplierWorkerPool[T]) KeyedSupplierExecutor[K, T] {
	return &defaultKeyedSupplierExecutor[K, T]{
		workerPool: workerPool,
	}
}

func (d *defaultKeyedSupplierExecutor[K, T]) ResolveAll(ctx context.Context, suppliers map[K]function.Supplier[T]) (map[K]T, error) {
	futureMap := map[K]async.Future[T]{}
	for key, toCall := range suppliers {
		futureMap[key] = d.getFuture(ctx, toCall)
	}
	resultMap := map[K]T{}
	for key, result := range futureMap {
		v, err := result.Get(ctx)
		if err != nil {
			return nil, err
		}
		resultMap[key] = v
	}
	return resultMap, nil
}

func (d *defaultKeyedSupplierExecutor[K, T]) getFuture(ctx context.Context, call function.Supplier[T]) async.Future[T] {
	return d.workerPool.Submit(ctx, call)
}

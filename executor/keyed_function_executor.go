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
	"github.com/palantir/witchcraft-go-tasks/workerpool"
)

// KeyedFunctionExecutor is syntactic sugar over KeyedSupplierExecutor for the common
// case where each task's result is fully determined by its key. The caller supplies a
// function.Function[K, T] and a slice of keys; each key is turned into a Supplier
// that invokes the function with that key, and the results are collected into a
// map[K]T.
//
// Inherits the fail-fast error semantics of KeyedSupplierExecutor.ResolveAll.
type KeyedFunctionExecutor[K comparable, T any] interface {
	GetAll(ctx context.Context, keys []K) (map[K]T, error)
}

type defaultKeyedFunctionExecutor[K comparable, T any] struct {
	underlyingFn          function.Function[K, T]
	keyedSupplierExecutor KeyedSupplierExecutor[K, T]
}

// NewDefaultKeyedFunctionExecutor returns the default implementation of
// KeyedFunctionExecutor[K, T]. The worker pool controls the maximum concurrency of
// execution of the underlying function.
func NewDefaultKeyedFunctionExecutor[K comparable, T any](
	underlyingFn function.Function[K, T],
	workerPool workerpool.SupplierWorkerPool[T]) KeyedFunctionExecutor[K, T] {
	return &defaultKeyedFunctionExecutor[K, T]{
		underlyingFn:          underlyingFn,
		keyedSupplierExecutor: NewDefaultKeyedSupplierExecutor[K, T](workerPool),
	}
}

func (d *defaultKeyedFunctionExecutor[K, T]) GetAll(ctx context.Context, keys []K) (map[K]T, error) {
	suppliers := map[K]function.Supplier[T]{}
	for _, key := range keys {
		suppliers[key] = d.getSupplierFunc(key)
	}
	return d.keyedSupplierExecutor.ResolveAll(ctx, suppliers)
}

func (d *defaultKeyedFunctionExecutor[K, T]) getSupplierFunc(key K) function.Supplier[T] {
	return function.NewSupplierFromFunc(func(ctx context.Context) (T, error) {
		return d.underlyingFn.Apply(ctx, key)
	})
}

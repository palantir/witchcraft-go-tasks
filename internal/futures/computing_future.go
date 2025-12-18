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

package futures

import (
	"context"
	"sync"

	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/util/async"
)

// ComputingFuture implements async.Future[T].
// In additional it exposes Compute(ctx context.Context). Compute is a synchronous function that will run the supplier
// The underlying Get function will not return until compute is finished
// ComputingFuture implements async.Future[T] and defines the Compute(ctx context.Context) function.
type ComputingFuture[T any] interface {
	async.Future[T]
	Compute(ctx context.Context)
}

type computationResult[T any] struct {
	result T
	err    error
}

// NewDefaultComputingFuture returns the default implementation of ComputingFuture[T].
// The Compute function of this implementation runs the provided supplier exactly once and stores the
// result. Any panics that occur in the supplier are caught and treated as an error of the supplier.
// The Get function of this implementation returns the result computed by Compute (and blocks until Compute has been called and completes if it has not yet been called).
// Neither the Compute nor the Get implementations check for context cancellation: if cancellation support is needed, it should be handled by the provided supplier.
func NewDefaultComputingFuture[T any](supplier function.Supplier[T]) ComputingFuture[T] {
	return &defaultComputingFuture[T]{
		cond:     sync.NewCond(&sync.Mutex{}),
		supplier: supplier,
	}
}

type defaultComputingFuture[T any] struct {
	supplier          function.Supplier[T]
	cond              *sync.Cond
	computationResult *computationResult[T]
}

func (d *defaultComputingFuture[T]) Compute(ctx context.Context) {
	d.cond.L.Lock()
	defer d.cond.L.Unlock()
	if d.computationResult != nil {
		return
	}
	catchPanic := func(ctx context.Context) error {
		result, err := d.supplier.Get(ctx)
		d.computationResult = &computationResult[T]{
			result: result,
			err:    err,
		}
		return nil
	}
	hasPanic := wapp.RunWithRecoveryLoggingWithError(ctx, catchPanic)
	if hasPanic != nil {
		d.computationResult = &computationResult[T]{
			err: hasPanic,
		}
	}
	d.cond.Broadcast()
}

func (d *defaultComputingFuture[T]) Get(ctx context.Context) (T, error) {
	d.cond.L.Lock()
	defer d.cond.L.Unlock()
	for d.computationResult == nil {
		d.cond.Wait()
	}
	return d.computationResult.result, d.computationResult.err
}

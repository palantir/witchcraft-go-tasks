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

	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/util/async"
)

type defaultVoidWorkerPool struct {
	workerPool WorkerPool[struct{}]
}

// NewDefaultVoidWorkerPool creates a default VoidWorkerPool
// The configuration options are passed into the underlying WorkerPool[struct{}]
func NewDefaultVoidWorkerPool(ctx context.Context, options ...Option) VoidWorkerPool {
	return &defaultVoidWorkerPool{
		workerPool: NewDefaultWorkerPool[struct{}](ctx, options...),
	}
}

func (d *defaultVoidWorkerPool) Submit(ctx context.Context, runnable function.Runnable) async.VoidFuture {
	supplier := function.NewSupplierFromFunc(func(ctx context.Context) (struct{}, error) {
		err := runnable.Run(ctx)
		return struct{}{}, err
	})
	return d.workerPool.Submit(ctx, supplier)
}

func (d *defaultVoidWorkerPool) SubmitWithCallback(ctx context.Context, runnable function.Runnable, onComplete func(error)) {
	d.workerPool.SubmitWithCallback(ctx, function.NewSupplierFromFunc(func(ctx context.Context) (struct{}, error) {
		err := runnable.Run(ctx)
		return struct{}{}, err
	}), func(_ struct{}, err error) {
		onComplete(err)
	})
}

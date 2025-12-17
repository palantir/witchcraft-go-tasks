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

type defaultProcessorWorkerPool[A, T any] struct {
	workerPool WorkerPool[T]
	processor  function.Function[A, T]
}

// NewDefaultProcessorWorkerPool creates a default ProcessorWorkerPool.
// Arguments to Submit are passed to the processor function to populate result futures.
func NewDefaultProcessorWorkerPool[A, T any](ctx context.Context, processor function.Function[A, T], options ...Option) ProcessorWorkerPool[A, T] {
	return &defaultProcessorWorkerPool[A, T]{
		workerPool: NewDefaultWorkerPool[T](ctx, options...),
		processor:  processor,
	}
}

func (d *defaultProcessorWorkerPool[A, T]) Submit(ctx context.Context, arg A) async.Future[T] {
	return d.workerPool.Submit(ctx, function.NewSupplierFromFunction[A, T](arg, d.processor))
}

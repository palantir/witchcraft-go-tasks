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

type defaultFunctionWorkerPool[T, R any] struct {
	workerPool SupplierWorkerPool[R]
	function   function.Function[T, R]
}

// NewDefaultFunctionWorkerPool creates a default FunctionWorkerPool.
func NewDefaultFunctionWorkerPool[T, R any](ctx context.Context, function function.Function[T, R], options ...Option) FunctionWorkerPool[T, R] {
	return &defaultFunctionWorkerPool[T, R]{
		workerPool: NewDefaultSupplierWorkerPool[R](ctx, options...),
		function:   function,
	}
}

func (d *defaultFunctionWorkerPool[T, R]) Submit(ctx context.Context, arg T) async.Future[R] {
	return d.workerPool.Submit(ctx, function.NewSupplierFromFunction[T, R](arg, d.function))
}

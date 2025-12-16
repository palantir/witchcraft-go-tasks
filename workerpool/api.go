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

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/util/async"
)

// WorkerPool is a generic interface that allows users to submit a Supplier and get back a future in return
// In general worker pools should not be used in a standalone manner, however they are exposed in a public package to provider underlying configuration options
type WorkerPool[T any] interface {
	Submit(ctx context.Context, supplier function.Supplier[T]) async.Future[T]
	SubmitWithCallback(ctx context.Context, supplier function.Supplier[T], onComplete func(T, error))
}

// ProcessorWorkerPool is a generic interface that allows users to submit an argument value and get back a future in return.
// Implementations return a future that will contain a result when the processing is done.
type ProcessorWorkerPool[A, T any] interface {
	Submit(ctx context.Context, arg A) async.Future[T]
	SubmitWithCallback(ctx context.Context, arg A, onComplete func(T, error))
}

// VoidWorkerPool is syntactic sugar over a WorkerPool. It is required because of the current lack of parameterized-methods
// It allows users to pass in a Runnable instead of a Supplier and hard-types the return Future to a VoidFuture
// It accepts the same configuration options that are passed to the underlying WorkerPool
type VoidWorkerPool interface {
	Submit(ctx context.Context, runnable function.Runnable) async.VoidFuture
	SubmitWithCallback(ctx context.Context, runnable function.Runnable, onComplete func(error))
}

// Config that is used to configure both the WorkerPool and VoidWorkerPool. Configured with given options
type Config struct {
	maxNumberOfWorkers *int
	tags               []metrics.Tag
}

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

// Package workerpool provides low-level worker pool primitives for concurrent task executor.
//
// Worker pools are advanced building blocks that manage goroutine lifecycles and task queuing.
// They are exported to provide maximum flexibility for advanced use cases, but most users
// should prefer higher-level abstractions built on top of these primitives.
//
// # Worker Pool Types
//
// Each worker pool type corresponds to a functional interface in the function package:
//
//   - SupplierWorkerPool[R]: Executes function.Supplier[R] tasks that produce a result R
//   - FunctionWorkerPool[T,R]: Binds a function.Function[T,R] and accepts arguments T, returning R
//   - RunnableWorkerPool: Executes function.Runnable tasks that return only an error
//   - ConsumerWorkerPool[T]: Binds a function.Consumer[T] and accepts arguments T
//
// # Concurrency Model
//
// Worker pools are preferred over spinning up bespoke goroutines because:
//   - Workers are reused across tasks, avoiding repeated goroutine creation overhead
//   - Concurrency can be bounded with WithMaxNumberOfThreads to prevent resource exhaustion
//
// Worker pools use dynamic scaling with optional limits:
//   - Workers (goroutines) are created on-demand when tasks are submitted
//   - By default, pools scale up unboundedly to match workload
//   - Use WithMaxNumberOfThreads to cap the maximum number of concurrent workers
//   - When all workers are busy, tasks queue until a worker becomes available
//   - Workers shut down gracefully when the parent context is cancelled
//
// # Submit vs SubmitWithCallback
//
// Submit returns a Future that can be awaited:
//
//	future := pool.Submit(ctx, supplier)
//	result, err := future.Get(ctx)  // Blocks until complete
//
// SubmitWithCallback is fire-and-forget with a completion callback:
//
//	pool.SubmitWithCallback(ctx, supplier, func(ctx context.Context, result R, err error) {
//	    // Called asynchronously when task completes
//	})
//
// Use Submit when you need the result synchronously or want to coordinate multiple futures.
// Use SubmitWithCallback for fire-and-forget patterns or when handling results asynchronously.
package workerpool

import (
	"context"

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/util/async"
)

// SupplierWorkerPool is a generic interface that allows users to submit a Supplier and get back a future in return
// In general worker pools should not be used in a standalone manner, however they are exposed in a public package to provider underlying configuration options
type SupplierWorkerPool[R any] interface {
	Submit(ctx context.Context, supplier function.Supplier[R]) async.Future[R]
	SubmitWithCallback(ctx context.Context, supplier function.Supplier[R], onComplete func(context.Context, R, error))
}

// FunctionWorkerPool is a generic interface that allows users to submit an argument value and get back a future in return.
// Implementations return a future that will contain a result when the processing is done.
type FunctionWorkerPool[T, R any] interface {
	Submit(ctx context.Context, arg T) async.Future[R]
	SubmitWithCallback(ctx context.Context, arg T, onComplete func(context.Context, T, R, error))
}

// RunnableWorkerPool is syntactic sugar over a WorkerPool. It is required because of the current lack of parameterized-methods
// It allows users to pass in a Runnable instead of a Supplier and hard-types the return Future to a VoidFuture
// It accepts the same configuration options that are passed to the underlying WorkerPool
type RunnableWorkerPool interface {
	Submit(ctx context.Context, runnable function.Runnable) async.VoidFuture
	SubmitWithCallback(ctx context.Context, runnable function.Runnable, onComplete func(context.Context, error))
}

type ConsumerWorkerPool[T any] interface {
	Submit(ctx context.Context, arg T) async.VoidFuture
	SubmitWithCallback(ctx context.Context, arg T, onComplete func(context.Context, T, error))
}

// Config that is used to configure both the WorkerPool and VoidWorkerPool. Configured with given options
type Config struct {
	maxNumberOfWorkers *int
	tags               []metrics.Tag
}

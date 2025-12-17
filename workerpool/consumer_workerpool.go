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

type defaultConsumerWorkerPool[T any] struct {
	workerPool RunnableWorkerPool
	consumer   function.Consumer[T]
}

// NewDefaultConsumerWorkerPool returns a default ConsumerWorkerPool[T]
func NewDefaultConsumerWorkerPool[T any](ctx context.Context, consumer function.Consumer[T], options ...Option) ConsumerWorkerPool[T] {
	return &defaultConsumerWorkerPool[T]{
		workerPool: NewDefaultRunnableWorkerPool(ctx, options...),
		consumer:   consumer,
	}
}

func (d defaultConsumerWorkerPool[T]) Submit(ctx context.Context, arg T) async.VoidFuture {
	return d.workerPool.Submit(ctx, function.NewRunnableFromConsumer(arg, d.consumer))
}

func (d *defaultConsumerWorkerPool[T]) SubmitWithCallback(ctx context.Context, arg T, onComplete func(context.Context, T, error)) {
	d.workerPool.Submit(ctx, function.NewRunnableFromFunc(func(ctx context.Context) error {
		err := d.consumer.Accept(ctx, arg)
		onComplete(ctx, arg, err)
		return err
	}))
}

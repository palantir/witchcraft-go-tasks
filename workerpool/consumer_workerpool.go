package workerpool

import (
	"context"

	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/util/async"
)

type defaultConsumerWorkerPool[T any] struct {
	workerPool RunnableWorkerPool
	processor  function.Consumer[T]
}

// NewDefaultConsumerWorkerPool returns a default ConsumerWorkerPool[T]
func NewDefaultConsumerWorkerPool[T any](ctx context.Context, processor function.Consumer[T], options ...Option) ConsumerWorkerPool[T] {
	return &defaultConsumerWorkerPool[T]{
		workerPool: NewDefaultRunnableWorkerPool(ctx, options...),
		processor:  processor,
	}
}

func (d defaultConsumerWorkerPool[T]) Submit(ctx context.Context, arg T) async.VoidFuture {
	return d.workerPool.Submit(ctx, function.NewRunnableFromConsumer(arg, d.processor))
}

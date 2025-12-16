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

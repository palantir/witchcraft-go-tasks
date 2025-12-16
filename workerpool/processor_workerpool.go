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

func (d *defaultProcessorWorkerPool[A, T]) SubmitWithCallback(ctx context.Context, arg A, onComplete func(T, error)) {
	d.workerPool.SubmitWithCallback(ctx, function.NewSupplierFromFunction[A, T](arg, d.processor), onComplete)
}

type consumerWorkerPool[T any] struct {
	workerPool VoidWorkerPool
	processor  function.Consumer[T]
}

type ConsumerWorkerPool[T any] interface {
	Submit(ctx context.Context, arg T) async.VoidFuture
	SubmitWithCallback(ctx context.Context, arg T, onComplete func(T, error))
}

func NewC[T any](ctx context.Context, processor function.Consumer[T], options ...Option) ConsumerWorkerPool[T] {
	return &consumerWorkerPool[T]{
		workerPool: NewDefaultVoidWorkerPool(ctx, options...),
		processor:  processor,
	}
}

func (d *consumerWorkerPool[T]) Submit(ctx context.Context, arg T) async.VoidFuture {
	return d.workerPool.Submit(ctx, function.NewRunnableFromConsumer(arg, d.processor))
}

func (d *consumerWorkerPool[T]) SubmitWithCallback(ctx context.Context, arg T, onComplete func(T, error)) {
	d.workerPool.Submit(ctx, function.NewRunnableFromFunc(func(ctx context.Context) error {
		err := d.processor.Accept(ctx, arg)
		onComplete(arg, err)
		return err
	}))
}

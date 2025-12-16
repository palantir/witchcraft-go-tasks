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

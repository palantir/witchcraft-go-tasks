package internal

import (
	"context"
	"sync"

	"github.com/palantir/witchcraft-go-logging/wlog/wapp"
	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/util/async"
)

// ComputingFuture implements async.Future[T].
// In additional it exposes Compute(ctx context.Context). Compute is a synchronous function that will run the supplier
// The underlying Get function will not return until compute is finished
// We explicitly do not check for context cancellation in this future and if the client for context timeouts to propagate their given supplier must handle it
type ComputingFuture[T any] interface {
	async.Future[T]
	Compute(ctx context.Context)
}

type computationResult[T any] struct {
	result T
	err    error
}

// NewDefaultComputingFuture returns the default implementation of ComputingFuture[T]
func NewDefaultComputingFuture[T any](supplier function.Supplier[T]) ComputingFuture[T] {
	return &defaultComputingFuture[T]{
		cond:     sync.NewCond(&sync.Mutex{}),
		supplier: supplier,
	}
}

type defaultComputingFuture[T any] struct {
	supplier          function.Supplier[T]
	cond              *sync.Cond
	computationResult *computationResult[T]
}

func (d *defaultComputingFuture[T]) Compute(ctx context.Context) {
	d.cond.L.Lock()
	defer d.cond.L.Unlock()
	if d.computationResult != nil {
		return
	}
	catchPanic := func(ctx context.Context) error {
		result, err := d.supplier.Get(ctx)
		d.computationResult = &computationResult[T]{
			result: result,
			err:    err,
		}
		return nil
	}
	hasPanic := wapp.RunWithRecoveryLoggingWithError(ctx, catchPanic)
	if hasPanic != nil {
		d.computationResult = &computationResult[T]{
			err: hasPanic,
		}
	}
	d.cond.Broadcast()
}

func (d *defaultComputingFuture[T]) Get(ctx context.Context) (T, error) {
	d.cond.L.Lock()
	defer d.cond.L.Unlock()
	for d.computationResult == nil {
		d.cond.Wait()
	}
	return d.computationResult.result, d.computationResult.err
}

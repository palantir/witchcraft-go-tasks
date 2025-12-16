package async

import (
	"context"
)

type functionalFuture[T any] struct {
	underlyingFunc func(ctx context.Context) (T, error)
}

// NewFunctionalFuture creates a new instance of functionalFuture which implements the Future[T] interface
// This implementation simply delegates out to the underlyingFunc when Get is called
func NewFunctionalFuture[T any](underlyingFunc func(ctx context.Context) (T, error)) Future[T] {
	return &functionalFuture[T]{
		underlyingFunc: underlyingFunc,
	}
}

func (f *functionalFuture[T]) Get(ctx context.Context) (T, error) {
	return f.underlyingFunc(ctx)
}

package async

import (
	"context"

	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/util/types"
)

// MapFuture enables mapping the result of the provided future to a new value.
// This is a convenience method for cases where the result of a Future is an input to another method - typically a constructor.
func MapFuture[T, R any](future Future[T], fn function.FunctionFunc[T, R]) Future[R] {
	return NewFunctionalFuture[R](func(ctx context.Context) (R, error) {
		val, err := future.Get(ctx)
		if err != nil {
			return types.Zero[R](), err
		}
		return fn(ctx, val)
	})
}

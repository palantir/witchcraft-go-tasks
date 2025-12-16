package function

import (
	"context"
)

// Function is a generic interface for applying a function that takes in type T and returns type R
// Decorated with a ctx context.Context for input and an error for returning
type Function[T any, R any] interface {
	Apply(ctx context.Context, arg T) (R, error)
}

// FunctionFunc is a type alias for a function that satisfies the Function interface
type FunctionFunc[T any, R any] func(ctx context.Context, arg T) (R, error)

func (f FunctionFunc[T, R]) Apply(ctx context.Context, arg T) (R, error) {
	return f(ctx, arg)
}

// NewFunctionFromFunc creates an interface of Function[T, R] by casing this given function to a FunctionFunc
func NewFunctionFromFunc[T any, R any](funcToCall func(ctx context.Context, arg T) (R, error)) Function[T, R] {
	return FunctionFunc[T, R](funcToCall)
}

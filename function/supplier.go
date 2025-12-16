package function

import (
	"context"
)

// Supplier is a generic interface for applying a function that returns type T
// Decorated with a ctx context.Context for input and an error for returning
type Supplier[T any] interface {
	Get(ctx context.Context) (T, error)
}

// SupplierFunc is a type alias for a function that satisfies the Supplier interface
type SupplierFunc[T any] func(ctx context.Context) (T, error)

func (f SupplierFunc[T]) Get(ctx context.Context) (T, error) {
	return f(ctx)
}

// NewSupplierFromFunc creates an interface of Supplier[T] by casing this given function to a SupplierFunc
func NewSupplierFromFunc[T any](funcToCall func(ctx context.Context) (T, error)) Supplier[T] {
	return SupplierFunc[T](funcToCall)
}

// NewSupplierFromFunction is a utility function that will create a Supplier[R] given an arg and a Function[T,R]
func NewSupplierFromFunction[T any, R any](arg T, function Function[T, R]) Supplier[R] {
	return NewSupplierFromFunc(func(ctx context.Context) (R, error) {
		return function.Apply(ctx, arg)
	})
}

// NewSupplierFromValue is a utility function that will create a Supplier[T] given a value of type T
func NewSupplierFromValue[T any](value T) Supplier[T] {
	return NewSupplierFromFunc(func(_ context.Context) (T, error) {
		return value, nil
	})
}

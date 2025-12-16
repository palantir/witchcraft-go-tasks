package function

import (
	"context"
)

// Consumer is a generic interface for applying a function that takes type T as an arg and returns nothing
// Decorated with a ctx context.Context for input and an error for returning
type Consumer[T any] interface {
	Accept(ctx context.Context, arg T) error
}

// ConsumerFunc is a type alias for a function that satisfies the Consumer interface
type ConsumerFunc[T any] func(ctx context.Context, arg T) error

func (f ConsumerFunc[T]) Accept(ctx context.Context, arg T) error {
	return f(ctx, arg)
}

// NewConsumerFromFunc creates an interface of Consumer[T] by casing this given function to a ConsumerFunc
func NewConsumerFromFunc[T any](funcToCall func(ctx context.Context, arg T) error) Consumer[T] {
	return ConsumerFunc[T](funcToCall)
}

// NewConsumerFromFunction is a utility function that will create a Consumer[R] given an arg and a Function[T,any]
func NewConsumerFromFunction[T, R any](function Function[T, R]) Consumer[T] {
	return NewConsumerFromFunc(func(ctx context.Context, arg T) error {
		_, err := function.Apply(ctx, arg)
		return err
	})
}

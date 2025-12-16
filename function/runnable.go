package function

import (
	"context"
)

// Runnable is a functional interface for running an underlying function
// Decorated with a ctx context.Context for input and an error for returning
type Runnable interface {
	Run(ctx context.Context) error
}

// NamedRunnable is a named object that can be run.
type NamedRunnable interface {
	Runnable
	Name() string
}

// RunnableFunc is a type alias for a function that satisfies the Runnable interface
type RunnableFunc func(ctx context.Context) error

// Run implements the Runnable interface
func (f RunnableFunc) Run(ctx context.Context) error {
	return f(ctx)
}

// NewRunnableFromFunc creates an interface of Runnable by casing this given function to a RunnableFunc
func NewRunnableFromFunc(funcToCall func(ctx context.Context) error) Runnable {
	return RunnableFunc(funcToCall)
}

// NewRunnableFromFunction is a utility function that will create a Runnable given an arg and a Function[T,any]
func NewRunnableFromFunction[T, R any](arg T, function Function[T, R]) Runnable {
	return NewRunnableFromConsumer(arg, NewConsumerFromFunction(function))
}

// NewRunnableFromConsumer is a utility function that will create a Runnable given an arg and a Consumer[T]
func NewRunnableFromConsumer[T any](arg T, consumer Consumer[T]) Runnable {
	return NewRunnableFromFunc(func(ctx context.Context) error {
		return consumer.Accept(ctx, arg)
	})
}

// NewRunnableFromSupplier is a utility function that will create a Runnable given an arg and a Supplier[any]
func NewRunnableFromSupplier[T any](c Supplier[T]) Runnable {
	return NewRunnableFromFunc(func(ctx context.Context) error {
		_, err := c.Get(ctx)
		return err
	})
}

// Copyright (c) 2025 Palantir Technologies. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

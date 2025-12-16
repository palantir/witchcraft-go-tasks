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

// Consumer is a generic interface for applying a function that takes type T as an arg and returns nothing
// Decorated with a ctx context.Context for input and an error for returning
type Consumer[T any] interface {
	Accept(ctx context.Context, arg T) error
}

// ConsumerFunc is a named type for a function that implements the Consumer interface.
type ConsumerFunc[T any] func(ctx context.Context, arg T) error

func (f ConsumerFunc[T]) Accept(ctx context.Context, arg T) error {
	return f(ctx, arg)
}

// NewConsumerFromFunc creates an interface of Consumer[T] by casing this given function to a ConsumerFunc
func NewConsumerFromFunc[T any](funcToCall func(ctx context.Context, arg T) error) Consumer[T] {
	return ConsumerFunc[T](funcToCall)
}

// NewConsumerFromFunction returns a Consumer[T] created from the provided Function[T, R]. The returned Consumer calls the Apply function of the provided Function with the T argument, ignores the R value returned by the function, and returns the error returned by the function.
func NewConsumerFromFunction[T, R any](function Function[T, R]) Consumer[T] {
	return NewConsumerFromFunc(func(ctx context.Context, arg T) error {
		_, err := function.Apply(ctx, arg)
		return err
	})
}

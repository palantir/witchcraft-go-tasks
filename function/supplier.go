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

// Supplier is a generic interface for applying a function that returns type T
// Decorated with a ctx context.Context for input and an error for returning
type Supplier[T any] interface {
	Get(ctx context.Context) (T, error)
}

// SupplierFunc is a named type for a function that implements the Supplier interface.
type SupplierFunc[T any] func(ctx context.Context) (T, error)

func (f SupplierFunc[T]) Get(ctx context.Context) (T, error) {
	return f(ctx)
}

// NewSupplierFromFunc returns a Supplier[T] by by using type conversion to convert the provided function to a  SupplierFunc.
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

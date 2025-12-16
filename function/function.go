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

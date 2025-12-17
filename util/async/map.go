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

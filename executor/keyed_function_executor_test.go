// Copyright (c) 2026 Palantir Technologies. All rights reserved.
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

package executor

import (
	"context"
	"testing"

	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/workerpool"
	"github.com/stretchr/testify/assert"
)

func TestKeyedFunctionExecutor_GetAll(t *testing.T) {
	ctx := context.Background()
	fetcher := function.NewFunctionFromFunc(func(ctx context.Context, s string) (int, error) {
		switch s {
		case "a":
			return 1, nil
		case "b":
			return 2, nil
		case "c":
			return 3, nil
		case "d":
			return 4, nil
		}
		return 5, nil
	})
	kfe := NewDefaultKeyedFunctionExecutor[string, int](fetcher, workerpool.NewDefaultSupplierWorkerPool[int](context.Background()))
	r, err := kfe.GetAll(ctx, []string{"a", "b", "c", "d", "e"})
	assert.NoError(t, err)
	assert.Equal(t, map[string]int{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, r)
}

func TestKeyedFunctionExecutor_CancelledContext(t *testing.T) {
	fetcher := function.NewFunctionFromFunc(func(ctx context.Context, s string) (int, error) {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 1, nil
	})
	kfe := NewDefaultKeyedFunctionExecutor[string, int](fetcher, workerpool.NewDefaultSupplierWorkerPool[int](context.Background()))
	ctx := context.Background()
	ctx2, cancel := context.WithCancel(ctx)
	cancel()
	assert.NoError(t, ctx.Err())
	assert.Error(t, ctx2.Err())
	result, err := kfe.GetAll(ctx, []string{"a"})
	assert.NoError(t, err)
	assert.Equal(t, map[string]int{"a": 1}, result)
	result, err = kfe.GetAll(ctx2, []string{"a"})
	assert.Error(t, err)
	assert.Empty(t, result)
	result, err = kfe.GetAll(ctx, []string{"a"})
	assert.NoError(t, err)
	assert.Equal(t, map[string]int{"a": 1}, result)
}

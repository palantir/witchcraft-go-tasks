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

	werror "github.com/palantir/witchcraft-go-error"
	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/workerpool"
	"github.com/stretchr/testify/assert"
)

func TestSupplierExecutorResolveAll(t *testing.T) {
	workerPool := workerpool.NewDefaultSupplierWorkerPool[string](context.Background())
	supplierExecutor := NewDefaultSupplierExecutor[string](workerPool)
	r1 := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "1", nil
	})
	r2 := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "2", nil
	})
	result, errs := supplierExecutor.ResolveAll(context.Background(), []function.Supplier[string]{r1, r2})
	assert.Empty(t, errs)
	assert.Equal(t, []string{"1", "2"}, result)
}

func TestSupplierExecutor_ResolveUntilError(t *testing.T) {
	workerPool := workerpool.NewDefaultSupplierWorkerPool[string](context.Background())
	supplierExecutor := NewDefaultSupplierExecutor[string](workerPool)
	r1 := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "1", nil
	})
	r2 := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "2", nil
	})
	result, err := supplierExecutor.ResolveUntilError(context.Background(), []function.Supplier[string]{r1, r2})
	assert.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, result)
}

func TestSupplierExecutorResolveAllSomeErrors(t *testing.T) {
	workerPool := workerpool.NewDefaultSupplierWorkerPool[string](context.Background())
	supplierExecutor := NewDefaultSupplierExecutor[string](workerPool)
	r1 := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "1", nil
	})
	r2 := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "", werror.Error("err")
	})
	r3 := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "3", nil
	})
	r4 := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "", werror.Error("err")
	})
	result, errs := supplierExecutor.ResolveAll(context.Background(), []function.Supplier[string]{r1, r2, r3, r4})
	assert.Equal(t, 2, len(errs))
	assert.Equal(t, []string{"1", "3"}, result)
}

func TestSupplierExecutorResolveUntilErrorSomeErrors(t *testing.T) {
	workerPool := workerpool.NewDefaultSupplierWorkerPool[string](context.Background())
	supplierExecutor := NewDefaultSupplierExecutor[string](workerPool)
	r1 := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "1", nil
	})
	r2 := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "", werror.Error("err")
	})
	r3 := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "3", nil
	})
	r4 := function.NewSupplierFromFunc(func(ctx context.Context) (string, error) {
		return "", werror.Error("err")
	})
	result, err := supplierExecutor.ResolveUntilError(context.Background(), []function.Supplier[string]{r1, r2, r3, r4})
	assert.Error(t, err)
	assert.Equal(t, []string(nil), result)
}

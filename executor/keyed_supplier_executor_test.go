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

func TestKeyedSupplierExecutor_ResolveAll(t *testing.T) {
	pool := workerpool.NewDefaultSupplierWorkerPool[string](context.Background())
	kse := NewDefaultKeyedSupplierExecutor[int, string](pool)
	suppliers := map[int]function.Supplier[string]{
		1: function.NewSupplierFromFunc(func(ctx context.Context) (string, error) { return "a", nil }),
		2: function.NewSupplierFromFunc(func(ctx context.Context) (string, error) { return "b", nil }),
		3: function.NewSupplierFromFunc(func(ctx context.Context) (string, error) { return "c", nil }),
	}
	result, err := kse.ResolveAll(context.Background(), suppliers)
	assert.NoError(t, err)
	assert.Equal(t, map[int]string{1: "a", 2: "b", 3: "c"}, result)
}

func TestKeyedSupplierExecutor_ResolveAll_FailsFastOnError(t *testing.T) {
	pool := workerpool.NewDefaultSupplierWorkerPool[string](context.Background())
	kse := NewDefaultKeyedSupplierExecutor[int, string](pool)
	suppliers := map[int]function.Supplier[string]{
		1: function.NewSupplierFromFunc(func(ctx context.Context) (string, error) { return "a", nil }),
		2: function.NewSupplierFromFunc(func(ctx context.Context) (string, error) { return "", werror.Error("boom") }),
	}
	result, err := kse.ResolveAll(context.Background(), suppliers)
	assert.Error(t, err)
	assert.Nil(t, result)
}

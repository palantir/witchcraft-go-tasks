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

package workerpool

import (
	"context"
	"strconv"
	"testing"

	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewProcessorWorkerPool(t *testing.T) {
	ctx := context.Background()
	processorPool := NewDefaultProcessorWorkerPool(ctx, function.NewFunctionFromFunc(func(ctx context.Context, arg string) (int, error) {
		return strconv.Atoi(arg)
	}))
	threeFuture := processorPool.Submit(ctx, "3")
	threeValue, err := threeFuture.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, threeValue)

	errFuture := processorPool.Submit(ctx, "a")
	errValue, err := errFuture.Get(ctx)
	require.Error(t, err)
	require.Zero(t, errValue)
}

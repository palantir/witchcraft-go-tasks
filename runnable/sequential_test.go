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

package runnable

import (
	"context"
	"fmt"
	"testing"

	"github.com/palantir/witchcraft-go-tasks/function"
	"github.com/palantir/witchcraft-go-tasks/internal/testcontext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSequentialRunnable(t *testing.T) {
	ctx := testcontext.GetTestContext(t)

	called1 := false
	runnable1 := New("runnable-1", func(ctx context.Context) error {
		called1 = true
		return nil
	})
	called2 := false
	runnable2 := New("runnable-2", func(ctx context.Context) error {
		called2 = true
		return nil
	})

	runnable := NewSequential("", []function.NamedRunnable{runnable1, runnable2})
	err := runnable.Run(ctx)
	require.NoError(t, err)
	assert.True(t, called1)
	assert.True(t, called2)
}

func TestSequentialRunnable_Error(t *testing.T) {
	ctx := testcontext.GetTestContext(t)

	called1 := false
	runnable1 := New("runnable-1", func(ctx context.Context) error {
		called1 = true
		return fmt.Errorf("error-1")
	})
	called2 := false
	runnable2 := New("runnable-2", func(ctx context.Context) error {
		called2 = true
		return fmt.Errorf("error-2")
	})

	runnable := NewSequential("", []function.NamedRunnable{runnable1, runnable2})
	err := runnable.Run(ctx)
	require.Error(t, err)
	assert.Equal(t, fmt.Errorf("error-1"), err)
	assert.True(t, called1)
	assert.False(t, called2)
}

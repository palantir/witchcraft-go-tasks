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

package async_test

import (
	"context"
	"errors"
	"testing"

	"github.com/palantir/witchcraft-go-tasks/util/async"
	"github.com/stretchr/testify/assert"
)

func TestMapFuture_Error(t *testing.T) {
	testFuture := &testFuture[string]{
		err: errors.New("initial future error"),
	}
	mappedFuture := async.MapFuture[string, int](testFuture, func(ctx context.Context, s string) (int, error) {
		return len(s), nil
	})
	got, err := mappedFuture.Get(context.Background())
	assert.EqualError(t, err, "initial future error")
	assert.Zero(t, got)
}

func TestMapFuture_Success(t *testing.T) {
	testFuture := &testFuture[string]{
		val: "value",
	}
	mappedFuture := async.MapFuture[string, int](testFuture, func(ctx context.Context, s string) (int, error) {
		return len(s), nil
	})
	got, err := mappedFuture.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 5, got)
}

type testFuture[T any] struct {
	val T
	err error
}

func (t testFuture[T]) Get(ctx context.Context) (T, error) {
	return t.val, t.err
}
